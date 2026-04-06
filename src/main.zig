const std = @import("std");

const app_version = "0.1.0";
const skipd_socket_path_default = "/tmp/.skipd_server_sock";
const skipd_magic = "magicv1 ";
const skipd_header_prefix = 16;
const max_skipd_frame = 16 * 1024 * 1024;

const Command = enum {
    list,
    stat,
    find,
    node2json,
    json2node,
    add_node,
    delete_node,
    delete_nodes,
    warm_cache,
    reorder,
    plan,
    version,
};

const OutputFormat = enum {
    table,
    json,
    jsonl,
    text,
};

const MutationMode = enum {
    append,
    replace,
};

const Options = struct {
    command: Command,
    query: QueryOptions = .{},
};

const QueryOptions = struct {
    ids_csv: ?[]const u8 = null,
    source: ?[]const u8 = null,
    source_tag: ?[]const u8 = null,
    airport_identity: ?[]const u8 = null,
    group: ?[]const u8 = null,
    protocol: ?[]const u8 = null,
    name: ?[]const u8 = null,
    identity: ?[]const u8 = null,
    format: ?[]const u8 = null,
    input: ?[]const u8 = null,
    position: ?[]const u8 = null,
    mode: ?[]const u8 = null,
    canonical: bool = false,
    schema2: bool = false,
    reuse_ids: bool = false,
    dry_run: bool = false,
    stdin_input: bool = false,
    all: bool = false,
    all_subscribe: bool = false,
    socket_path: []const u8 = skipd_socket_path_default,
};

const DbPair = struct {
    key: []u8,
    value: []u8,

    fn deinit(self: *DbPair, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
    }
};

const SkipdClient = struct {
    fd: std.posix.fd_t,

    fn connect(socket_path: []const u8) !SkipdClient {
        const address = try std.net.Address.initUnix(socket_path);
        const fd = try std.posix.socket(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(fd);
        try std.posix.connect(fd, &address.any, address.getOsSockLen());
        return .{ .fd = fd };
    }

    fn deinit(self: *SkipdClient) void {
        std.posix.close(self.fd);
    }

    fn writeAll(self: *SkipdClient, bytes: []const u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const written = try std.posix.write(self.fd, bytes[offset..]);
            if (written == 0) return error.WriteFailed;
            offset += written;
        }
    }

    fn readExact(self: *SkipdClient, bytes: []u8) !void {
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = try std.posix.read(self.fd, bytes[offset..]);
            if (n == 0) return error.UnexpectedEof;
            offset += n;
        }
    }

    fn sendCommand(self: *SkipdClient, allocator: std.mem.Allocator, command: []const u8, arg: []const u8) !void {
        const body = if (arg.len > 0)
            try std.fmt.allocPrint(allocator, "{s} {s}\n", .{ command, arg })
        else
            try std.fmt.allocPrint(allocator, "{s}\n", .{command});
        defer allocator.free(body);

        const request = try std.fmt.allocPrint(allocator, "{s}{d:0>7} {s}", .{ skipd_magic, body.len, body });
        defer allocator.free(request);
        try self.writeAll(request);
    }

    fn readFrameBodyAlloc(self: *SkipdClient, allocator: std.mem.Allocator) ![]u8 {
        var header: [skipd_header_prefix]u8 = undefined;
        try self.readExact(&header);
        if (!std.mem.eql(u8, header[0..skipd_magic.len], skipd_magic)) {
            return error.InvalidSkipdHeader;
        }
        const len = try std.fmt.parseInt(usize, header[skipd_magic.len .. skipd_magic.len + 7], 10);
        if (len > max_skipd_frame) return error.FrameTooLarge;

        const body = try allocator.alloc(u8, len);
        errdefer allocator.free(body);
        try self.readExact(body);
        return body;
    }

    fn getAlloc(self: *SkipdClient, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
        try self.sendCommand(allocator, "get", key);
        const body = try self.readFrameBodyAlloc(allocator);
        errdefer allocator.free(body);

        const parsed = try parseSkipdBody(body);
        if (!std.mem.eql(u8, parsed.command, "get")) return error.InvalidSkipdBody;
        if (!std.mem.eql(u8, parsed.key, key)) return error.InvalidSkipdBody;
        if (std.mem.eql(u8, parsed.value, "none")) {
            allocator.free(body);
            return null;
        }

        const value = try allocator.dupe(u8, parsed.value);
        allocator.free(body);
        return value;
    }

    fn listAlloc(self: *SkipdClient, allocator: std.mem.Allocator, prefix: []const u8) ![]DbPair {
        try self.sendCommand(allocator, "list", prefix);

        var pairs = std.ArrayList(DbPair){};
        errdefer {
            for (pairs.items) |*pair| pair.deinit(allocator);
            pairs.deinit(allocator);
        }

        while (true) {
            const body = try self.readFrameBodyAlloc(allocator);
            defer allocator.free(body);
            if (std.mem.indexOf(u8, body, "__end__") != null) break;

            const parsed = try parseSkipdBody(body);
            if (!std.mem.eql(u8, parsed.command, "list")) return error.InvalidSkipdBody;
            try pairs.append(allocator, .{
                .key = try allocator.dupe(u8, parsed.key),
                .value = try allocator.dupe(u8, parsed.value),
            });
        }

        return try pairs.toOwnedSlice(allocator);
    }

    fn ignoreResult(self: *SkipdClient, allocator: std.mem.Allocator) !void {
        const body = try self.readFrameBodyAlloc(allocator);
        allocator.free(body);
    }

    fn setValue(self: *SkipdClient, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        if (value.len == 0) {
            try self.removeKey(allocator, key);
            return;
        }
        const arg = try std.fmt.allocPrint(allocator, "{s} {s}", .{ key, value });
        defer allocator.free(arg);
        try self.sendCommand(allocator, "set", arg);
        try self.ignoreResult(allocator);
    }

    fn removeKey(self: *SkipdClient, allocator: std.mem.Allocator, key: []const u8) !void {
        try self.sendCommand(allocator, "remove", key);
        try self.ignoreResult(allocator);
    }
};

const SkipdBody = struct {
    command: []const u8,
    key: []const u8,
    value: []const u8,
};

const NodeJson = struct {
    _id: ?[]const u8 = null,
    _schema: ?u64 = null,
    _rev: ?u64 = null,
    _source: ?[]const u8 = null,
    _airport_identity: ?[]const u8 = null,
    _source_scope: ?[]const u8 = null,
    _source_url_hash: ?[]const u8 = null,
    _identity: ?[]const u8 = null,
    _identity_primary: ?[]const u8 = null,
    _identity_secondary: ?[]const u8 = null,
    _created_at: ?u64 = null,
    _updated_at: ?u64 = null,
    group: ?[]const u8 = null,
    name: ?[]const u8 = null,
    type: ?[]const u8 = null,
    xray_prot: ?[]const u8 = null,
    server: ?[]const u8 = null,
    port: ?[]const u8 = null,
    naive_server: ?[]const u8 = null,
    naive_port: ?[]const u8 = null,
    hy2_server: ?[]const u8 = null,
    hy2_port: ?[]const u8 = null,
};

const NodeRecord = struct {
    id: []u8,
    type_id: []u8,
    protocol: []u8,
    protocol_label: []u8,
    name: []u8,
    group: []u8,
    source: []u8,
    source_tag: []u8,
    airport_identity: []u8,
    source_scope: []u8,
    source_url_hash: []u8,
    identity: []u8,
    identity_primary: []u8,
    identity_secondary: []u8,
    server: []u8,
    port: []u8,
    schema: u64,
    rev: u64,
    created_at: u64,
    updated_at: u64,
    raw_json: []u8,

    fn deinit(self: *NodeRecord, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.type_id);
        allocator.free(self.protocol);
        allocator.free(self.protocol_label);
        allocator.free(self.name);
        allocator.free(self.group);
        allocator.free(self.source);
        allocator.free(self.source_tag);
        allocator.free(self.airport_identity);
        allocator.free(self.source_scope);
        allocator.free(self.source_url_hash);
        allocator.free(self.identity);
        allocator.free(self.identity_primary);
        allocator.free(self.identity_secondary);
        allocator.free(self.server);
        allocator.free(self.port);
        allocator.free(self.raw_json);
    }
};

const SchemaState = struct {
    nodes: []NodeRecord,
    current_id: []u8,
    current_identity: []u8,
    failover_id: []u8,
    failover_identity: []u8,
    next_id: usize,

    fn deinit(self: *SchemaState, allocator: std.mem.Allocator) void {
        freeNodeSlice(allocator, self.nodes);
        allocator.free(self.current_id);
        allocator.free(self.current_identity);
        allocator.free(self.failover_id);
        allocator.free(self.failover_identity);
    }
};

const StatItem = struct {
    key: []u8,
    count: usize,

    fn deinit(self: *StatItem, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
    }
};

pub fn main() void {
    run() catch |err| {
        const stderr = std.fs.File.stderr().deprecatedWriter();
        switch (err) {
            error.HelpRequested => {
                printUsage(std.fs.File.stdout().deprecatedWriter()) catch {};
                std.process.exit(0);
            },
            error.InvalidArguments => {
                printUsage(stderr) catch {};
            },
            error.UnsupportedSchema => {
                stderr.print("error: node-tool currently only supports schema2 storage\n", .{}) catch {};
            },
            else => {
                stderr.print("error: {s}\n", .{@errorName(err)}) catch {};
            },
        }
        std.process.exit(1);
    };
}

fn run() !void {
    const allocator = std.heap.c_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const options = try parseArgs(args);
    switch (options.command) {
        .version => {
            try std.fs.File.stdout().writeAll(app_version);
            try std.fs.File.stdout().writeAll("\n");
        },
        .list, .stat, .find, .node2json => {
            var nodes = try loadSchema2Nodes(allocator, options.query.socket_path);
            defer freeNodeSlice(allocator, nodes);
            try filterNodeSliceInPlace(allocator, &nodes, options.query);

            switch (options.command) {
                .list => try runList(allocator, nodes, options.query),
                .stat => try runStat(allocator, nodes, options.query),
                .find => try runFind(allocator, nodes, options.query),
                .node2json => try runNode2Json(allocator, nodes, options.query),
                else => unreachable,
            }
        },
        .json2node, .add_node, .delete_node, .delete_nodes => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            switch (options.command) {
                .json2node => try runJson2Node(allocator, &state, options.query),
                .add_node => try runAddNode(allocator, &state, options.query),
                .delete_node => try runDeleteNode(allocator, &state, options.query),
                .delete_nodes => try runDeleteNodes(allocator, &state, options.query),
                else => unreachable,
            }
        },
        .warm_cache => try runStub("warm-cache", options.query),
        .reorder => try runStub("reorder", options.query),
        .plan => try runStub("plan", options.query),
    }
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) return error.InvalidArguments;

    const command = blk: {
        if (std.mem.eql(u8, args[1], "-h") or std.mem.eql(u8, args[1], "--help")) return error.HelpRequested;
        if (std.mem.eql(u8, args[1], "list")) break :blk Command.list;
        if (std.mem.eql(u8, args[1], "stat")) break :blk Command.stat;
        if (std.mem.eql(u8, args[1], "find")) break :blk Command.find;
        if (std.mem.eql(u8, args[1], "node2json")) break :blk Command.node2json;
        if (std.mem.eql(u8, args[1], "json2node")) break :blk Command.json2node;
        if (std.mem.eql(u8, args[1], "add-node")) break :blk Command.add_node;
        if (std.mem.eql(u8, args[1], "delete-node")) break :blk Command.delete_node;
        if (std.mem.eql(u8, args[1], "delete-nodes")) break :blk Command.delete_nodes;
        if (std.mem.eql(u8, args[1], "warm-cache")) break :blk Command.warm_cache;
        if (std.mem.eql(u8, args[1], "reorder")) break :blk Command.reorder;
        if (std.mem.eql(u8, args[1], "plan")) break :blk Command.plan;
        if (std.mem.eql(u8, args[1], "version")) break :blk Command.version;
        return error.InvalidArguments;
    };

    var query = QueryOptions{};
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            try printCommandUsage(command, std.fs.File.stdout().deprecatedWriter());
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--ids")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.ids_csv = args[i];
        } else if (std.mem.eql(u8, arg, "--source")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.source = args[i];
        } else if (std.mem.eql(u8, arg, "--source-tag")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.source_tag = args[i];
        } else if (std.mem.eql(u8, arg, "--airport-identity")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.airport_identity = args[i];
        } else if (std.mem.eql(u8, arg, "--group")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.group = args[i];
        } else if (std.mem.eql(u8, arg, "--protocol")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.protocol = args[i];
        } else if (std.mem.eql(u8, arg, "--name")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.name = args[i];
        } else if (std.mem.eql(u8, arg, "--identity")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.identity = args[i];
        } else if (std.mem.eql(u8, arg, "--format")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.format = args[i];
        } else if (std.mem.eql(u8, arg, "--input")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.input = args[i];
        } else if (std.mem.eql(u8, arg, "--stdin")) {
            query.stdin_input = true;
        } else if (std.mem.eql(u8, arg, "--position")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.position = args[i];
        } else if (std.mem.eql(u8, arg, "--mode")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.mode = args[i];
        } else if (std.mem.eql(u8, arg, "--canonical")) {
            query.canonical = true;
        } else if (std.mem.eql(u8, arg, "--schema2")) {
            query.schema2 = true;
        } else if (std.mem.eql(u8, arg, "--reuse-ids")) {
            query.reuse_ids = true;
        } else if (std.mem.eql(u8, arg, "--dry-run")) {
            query.dry_run = true;
        } else if (std.mem.eql(u8, arg, "--all")) {
            query.all = true;
        } else if (std.mem.eql(u8, arg, "--all-subscribe")) {
            query.all_subscribe = true;
        } else if (std.mem.eql(u8, arg, "--socket")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.socket_path = args[i];
        } else {
            return error.InvalidArguments;
        }
    }

    return .{
        .command = command,
        .query = query,
    };
}

fn runList(allocator: std.mem.Allocator, nodes: []NodeRecord, query: QueryOptions) !void {
    const format = try resolveListFormat(query.format);
    switch (format) {
        .table => try writeListTable(std.fs.File.stdout().deprecatedWriter(), nodes),
        .json => try writeListJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, false),
        .jsonl => try writeListJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, true),
        else => unreachable,
    }
}

fn runFind(allocator: std.mem.Allocator, nodes: []NodeRecord, query: QueryOptions) !void {
    const format = try resolveListFormat(query.format);
    switch (format) {
        .table => try writeListTable(std.fs.File.stdout().deprecatedWriter(), nodes),
        .json => try writeListJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, false),
        .jsonl => try writeListJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, true),
        else => unreachable,
    }
}

fn runNode2Json(allocator: std.mem.Allocator, nodes: []NodeRecord, query: QueryOptions) !void {
    const format = try resolveNode2JsonFormat(query.format);
    if (query.canonical) {
        switch (format) {
            .json => try writeCanonicalJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, false),
            .jsonl => try writeCanonicalJson(allocator, std.fs.File.stdout().deprecatedWriter(), nodes, true),
            else => unreachable,
        }
        return;
    }

    switch (format) {
        .json => {
            const stdout = std.fs.File.stdout().deprecatedWriter();
            try stdout.writeAll("[\n");
            for (nodes, 0..) |node, idx| {
                if (idx > 0) try stdout.writeAll(",\n");
                try stdout.writeAll(node.raw_json);
            }
            try stdout.writeAll("\n]\n");
        },
        .jsonl => {
            const stdout = std.fs.File.stdout().deprecatedWriter();
            for (nodes) |node| {
                try stdout.writeAll(node.raw_json);
                try stdout.writeAll("\n");
            }
        },
        else => unreachable,
    }
}

fn runStat(allocator: std.mem.Allocator, nodes: []NodeRecord, query: QueryOptions) !void {
    const format = try resolveStatFormat(query.format);
    var protocols = std.ArrayList(StatItem){};
    defer freeStatItems(allocator, &protocols);
    var airports = std.ArrayList(StatItem){};
    defer freeStatItems(allocator, &airports);
    var sources = std.ArrayList(StatItem){};
    defer freeStatItems(allocator, &sources);

    var user_count: usize = 0;
    var subscribe_count: usize = 0;

    for (nodes) |node| {
        if (std.mem.eql(u8, node.source, "subscribe")) {
            subscribe_count += 1;
        } else {
            user_count += 1;
        }
        try bumpStatItem(allocator, &protocols, node.protocol);
        try bumpStatItem(allocator, &airports, if (node.airport_identity.len > 0) node.airport_identity else "local");
        try bumpStatItem(allocator, &sources, if (node.source.len > 0) node.source else "user");
    }

    switch (format) {
        .text => {
            const stdout = std.fs.File.stdout().deprecatedWriter();
            try stdout.print("total: {d}\n", .{nodes.len});
            try stdout.print("user: {d}\n", .{user_count});
            try stdout.print("subscribe: {d}\n", .{subscribe_count});
            try stdout.writeAll("protocols:\n");
            for (protocols.items) |item| try stdout.print("  {s}: {d}\n", .{ item.key, item.count });
            try stdout.writeAll("airports:\n");
            for (airports.items) |item| try stdout.print("  {s}: {d}\n", .{ item.key, item.count });
            try stdout.writeAll("sources:\n");
            for (sources.items) |item| try stdout.print("  {s}: {d}\n", .{ item.key, item.count });
        },
        .json => {
            const stdout = std.fs.File.stdout().deprecatedWriter();
            try stdout.writeAll("{");
            try stdout.print("\"total\":{d},\"user\":{d},\"subscribe\":{d},", .{ nodes.len, user_count, subscribe_count });
            try stdout.writeAll("\"protocols\":[");
            try writeStatArray(stdout, protocols.items);
            try stdout.writeAll("],\"airports\":[");
            try writeStatArray(stdout, airports.items);
            try stdout.writeAll("],\"sources\":[");
            try writeStatArray(stdout, sources.items);
            try stdout.writeAll("]}\n");
        },
        else => unreachable,
    }
}

fn runJson2Node(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);

    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const mode = resolveMutationMode(query.mode);
    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);

    var added: usize = 0;
    var updated: usize = 0;
    var removed: usize = 0;

    if (mode == .replace) {
        removed = state.nodes.len;
        freeNodeSlice(allocator, state.nodes);
        state.nodes = try allocator.alloc(NodeRecord, 0);
    }

    for (docs) |raw_doc| {
        try importNodeDocument(allocator, state, raw_doc, query, "tail", &added, &updated);
    }

    if (mode == .replace and state.nodes.len < removed) {
        removed -= state.nodes.len;
    } else if (mode == .replace) {
        removed = old_nodes.len - state.nodes.len + added + updated;
    }

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("json2node", added, updated, removed, state.nodes.len, query.dry_run);
}

fn runAddNode(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);
    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);
    if (docs.len != 1) return error.InvalidArguments;

    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    var added: usize = 0;
    var updated: usize = 0;
    const position = query.position orelse "tail";
    try importNodeDocument(allocator, state, docs[0], query, position, &added, &updated);

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("add-node", added, updated, 0, state.nodes.len, query.dry_run);
}

fn runDeleteNode(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const removed = try deleteMatchingNodes(allocator, state, query, true);
    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("delete-node", 0, 0, removed, state.nodes.len, query.dry_run);
}

fn runDeleteNodes(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const removed = try deleteMatchingNodes(allocator, state, query, false);
    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("delete-nodes", 0, 0, removed, state.nodes.len, query.dry_run);
}

fn runStub(command_name: []const u8, query: QueryOptions) !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("node-tool: {s} is planned but not implemented yet\n", .{command_name});
    if (query.ids_csv) |ids| try stdout.print("ids: {s}\n", .{ids});
}

fn resolveListFormat(raw: ?[]const u8) !OutputFormat {
    const format = raw orelse "table";
    if (std.ascii.eqlIgnoreCase(format, "table")) return .table;
    if (std.ascii.eqlIgnoreCase(format, "json")) return .json;
    if (std.ascii.eqlIgnoreCase(format, "jsonl")) return .jsonl;
    return error.InvalidArguments;
}

fn resolveNode2JsonFormat(raw: ?[]const u8) !OutputFormat {
    const format = raw orelse "json";
    if (std.ascii.eqlIgnoreCase(format, "json")) return .json;
    if (std.ascii.eqlIgnoreCase(format, "jsonl")) return .jsonl;
    return error.InvalidArguments;
}

fn resolveStatFormat(raw: ?[]const u8) !OutputFormat {
    const format = raw orelse "text";
    if (std.ascii.eqlIgnoreCase(format, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(format, "json")) return .json;
    return error.InvalidArguments;
}

fn loadSchema2Nodes(allocator: std.mem.Allocator, socket_path: []const u8) ![]NodeRecord {
    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();

    const schema_value = try client.getAlloc(allocator, "fss_data_schema");
    defer if (schema_value) |value| allocator.free(value);
    if (schema_value == null or !std.mem.eql(u8, schema_value.?, "2")) {
        return error.UnsupportedSchema;
    }

    const order_csv = try client.getAlloc(allocator, "fss_node_order");
    defer if (order_csv) |value| allocator.free(value);

    const pairs = try client.listAlloc(allocator, "fss_node_");
    defer {
        for (pairs) |*pair| pair.deinit(allocator);
        allocator.free(pairs);
    }

    var nodes = std.ArrayList(NodeRecord){};
    errdefer {
        for (nodes.items) |*node| node.deinit(allocator);
        nodes.deinit(allocator);
    }

    for (pairs) |pair| {
        if (!isNumericNodeKey(pair.key)) continue;
        try nodes.append(allocator, try parseNodeRecord(allocator, pair.key, pair.value));
    }

    return try reorderNodeRecordsAlloc(allocator, nodes.items, order_csv);
}

fn loadSchema2State(allocator: std.mem.Allocator, socket_path: []const u8) !SchemaState {
    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();

    const schema_value = try client.getAlloc(allocator, "fss_data_schema");
    defer if (schema_value) |value| allocator.free(value);
    if (schema_value == null or !std.mem.eql(u8, schema_value.?, "2")) {
        return error.UnsupportedSchema;
    }

    const order_csv = try client.getAlloc(allocator, "fss_node_order");
    defer if (order_csv) |value| allocator.free(value);

    const pairs = try client.listAlloc(allocator, "fss_node_");
    defer {
        for (pairs) |*pair| pair.deinit(allocator);
        allocator.free(pairs);
    }

    var nodes = std.ArrayList(NodeRecord){};
    errdefer {
        for (nodes.items) |*node| node.deinit(allocator);
        nodes.deinit(allocator);
    }
    for (pairs) |pair| {
        if (!isNumericNodeKey(pair.key)) continue;
        try nodes.append(allocator, try parseNodeRecord(allocator, pair.key, pair.value));
    }

    const reordered = try reorderNodeRecordsAlloc(allocator, nodes.items, order_csv);
    const current_id = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_current"));
    const current_identity = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_current_identity"));
    const failover_id = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_failover_backup"));
    const failover_identity = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_failover_identity"));
    const next_id_value = try client.getAlloc(allocator, "fss_node_next_id");
    defer if (next_id_value) |value| allocator.free(value);

    return .{
        .nodes = reordered,
        .current_id = current_id,
        .current_identity = current_identity,
        .failover_id = failover_id,
        .failover_identity = failover_identity,
        .next_id = if (next_id_value) |value| parsePositiveInt(value) catch (computeNextId(reordered)) else computeNextId(reordered),
    };
}

fn dupOrEmpty(allocator: std.mem.Allocator, value: ?[]u8) ![]u8 {
    defer if (value) |buf| allocator.free(buf);
    return try allocator.dupe(u8, if (value) |buf| buf else "");
}

fn parsePositiveInt(raw: []const u8) !usize {
    const trimmed = std.mem.trim(u8, raw, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidInteger;
    return try std.fmt.parseInt(usize, trimmed, 10);
}

fn computeNextId(nodes: []const NodeRecord) usize {
    var max_id: usize = 0;
    for (nodes) |node| {
        const value = std.fmt.parseInt(usize, node.id, 10) catch continue;
        if (value > max_id) max_id = value;
    }
    return max_id + 1;
}

fn nowMs() u64 {
    return @as(u64, @intCast(std.time.milliTimestamp()));
}

fn findNodeIndexById(nodes: []const NodeRecord, id: []const u8) ?usize {
    for (nodes, 0..) |node, idx| {
        if (std.mem.eql(u8, node.id, id)) return idx;
    }
    return null;
}

fn findNodeIndexByIdentity(nodes: []const NodeRecord, identity: []const u8) ?usize {
    for (nodes, 0..) |node, idx| {
        if (std.mem.eql(u8, node.identity, identity)) return idx;
    }
    return null;
}

fn readStreamCompatAlloc(allocator: std.mem.Allocator, file: std.fs.File, max_bytes: usize) ![]u8 {
    var list = std.ArrayList(u8){};
    defer list.deinit(allocator);

    var buf: [4096]u8 = undefined;
    while (true) {
        const n = try file.read(&buf);
        if (n == 0) break;
        if (list.items.len + n > max_bytes) return error.FileTooBig;
        try list.appendSlice(allocator, buf[0..n]);
    }

    return try list.toOwnedSlice(allocator);
}

fn readInputCompatAlloc(allocator: std.mem.Allocator, input_path: ?[]const u8, use_stdin: bool, max_bytes: usize) ![]u8 {
    if (use_stdin or input_path == null) {
        return try readStreamCompatAlloc(allocator, std.fs.File.stdin(), max_bytes);
    }
    const file = try std.fs.cwd().openFile(input_path.?, .{});
    defer file.close();
    return try readStreamCompatAlloc(allocator, file, max_bytes);
}

fn writeSchema2State(allocator: std.mem.Allocator, socket_path: []const u8, old_nodes: []const NodeRecord, state: *SchemaState) !void {
    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();

    try reconcileReferenceState(allocator, state);

    for (old_nodes) |old_node| {
        if (findNodeIndexById(state.nodes, old_node.id) == null) {
            const key = try std.fmt.allocPrint(allocator, "fss_node_{s}", .{old_node.id});
            defer allocator.free(key);
            try client.removeKey(allocator, key);
        }
    }

    for (state.nodes) |node| {
        const key = try std.fmt.allocPrint(allocator, "fss_node_{s}", .{node.id});
        defer allocator.free(key);
        const encoded = try encodeBase64StandardAlloc(allocator, node.raw_json);
        defer allocator.free(encoded);
        try client.setValue(allocator, key, encoded);
    }

    const order_csv = try joinNodeOrderCsvAlloc(allocator, state.nodes);
    defer allocator.free(order_csv);
    if (order_csv.len > 0) {
        try client.setValue(allocator, "fss_node_order", order_csv);
    } else {
        try client.removeKey(allocator, "fss_node_order");
    }

    const next_id_str = try std.fmt.allocPrint(allocator, "{d}", .{computeNextId(state.nodes)});
    defer allocator.free(next_id_str);
    try client.setValue(allocator, "fss_node_next_id", next_id_str);
    try client.setValue(allocator, "fss_data_schema", "2");

    const ts = nowMs();
    const ts_str = try std.fmt.allocPrint(allocator, "{d}", .{ts});
    defer allocator.free(ts_str);
    try client.setValue(allocator, "fss_node_catalog_ts", ts_str);
    try client.setValue(allocator, "fss_node_config_ts", ts_str);

    if (state.current_id.len > 0) {
        try client.setValue(allocator, "fss_node_current", state.current_id);
        try client.setValue(allocator, "fss_node_current_identity", state.current_identity);
    } else {
        try client.removeKey(allocator, "fss_node_current");
        try client.removeKey(allocator, "fss_node_current_identity");
    }

    if (state.failover_id.len > 0) {
        try client.setValue(allocator, "fss_node_failover_backup", state.failover_id);
        try client.setValue(allocator, "fss_node_failover_identity", state.failover_identity);
    } else {
        try client.removeKey(allocator, "fss_node_failover_backup");
        try client.removeKey(allocator, "fss_node_failover_identity");
    }
}

fn reconcileReferenceState(allocator: std.mem.Allocator, state: *SchemaState) !void {
    const current_idx = if (state.current_identity.len > 0)
        findNodeIndexByIdentity(state.nodes, state.current_identity)
    else if (state.current_id.len > 0)
        findNodeIndexById(state.nodes, state.current_id)
    else
        null;

    if (state.nodes.len == 0) {
        allocator.free(state.current_id);
        allocator.free(state.current_identity);
        allocator.free(state.failover_id);
        allocator.free(state.failover_identity);
        state.current_id = try allocator.dupe(u8, "");
        state.current_identity = try allocator.dupe(u8, "");
        state.failover_id = try allocator.dupe(u8, "");
        state.failover_identity = try allocator.dupe(u8, "");
        return;
    }

    const resolved_current = current_idx orelse 0;
    try replaceOwnedString(allocator, &state.current_id, state.nodes[resolved_current].id);
    try replaceOwnedString(allocator, &state.current_identity, state.nodes[resolved_current].identity);

    const failover_idx = if (state.failover_identity.len > 0)
        findNodeIndexByIdentity(state.nodes, state.failover_identity)
    else if (state.failover_id.len > 0)
        findNodeIndexById(state.nodes, state.failover_id)
    else
        null;

    if (failover_idx) |idx| {
        try replaceOwnedString(allocator, &state.failover_id, state.nodes[idx].id);
        try replaceOwnedString(allocator, &state.failover_identity, state.nodes[idx].identity);
    } else {
        try replaceOwnedString(allocator, &state.failover_id, "");
        try replaceOwnedString(allocator, &state.failover_identity, "");
    }
}

fn replaceOwnedString(allocator: std.mem.Allocator, target: *[]u8, value: []const u8) !void {
    allocator.free(target.*);
    target.* = try allocator.dupe(u8, value);
}

fn joinNodeOrderCsvAlloc(allocator: std.mem.Allocator, nodes: []const NodeRecord) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    for (nodes, 0..) |node, idx| {
        if (idx > 0) try writer.writeByte(',');
        try writer.writeAll(node.id);
    }
    return try out.toOwnedSlice(allocator);
}

fn resolveMutationMode(raw: ?[]const u8) MutationMode {
    const value = raw orelse "append";
    if (std.ascii.eqlIgnoreCase(value, "replace")) return .replace;
    return .append;
}

fn cloneNodeSlice(allocator: std.mem.Allocator, nodes: []const NodeRecord) ![]NodeRecord {
    var out = std.ArrayList(NodeRecord){};
    errdefer {
        for (out.items) |*node| node.deinit(allocator);
        out.deinit(allocator);
    }
    for (nodes) |node| {
        try out.append(allocator, .{
            .id = try allocator.dupe(u8, node.id),
            .type_id = try allocator.dupe(u8, node.type_id),
            .protocol = try allocator.dupe(u8, node.protocol),
            .protocol_label = try allocator.dupe(u8, node.protocol_label),
            .name = try allocator.dupe(u8, node.name),
            .group = try allocator.dupe(u8, node.group),
            .source = try allocator.dupe(u8, node.source),
            .source_tag = try allocator.dupe(u8, node.source_tag),
            .airport_identity = try allocator.dupe(u8, node.airport_identity),
            .source_scope = try allocator.dupe(u8, node.source_scope),
            .source_url_hash = try allocator.dupe(u8, node.source_url_hash),
            .identity = try allocator.dupe(u8, node.identity),
            .identity_primary = try allocator.dupe(u8, node.identity_primary),
            .identity_secondary = try allocator.dupe(u8, node.identity_secondary),
            .server = try allocator.dupe(u8, node.server),
            .port = try allocator.dupe(u8, node.port),
            .schema = node.schema,
            .rev = node.rev,
            .created_at = node.created_at,
            .updated_at = node.updated_at,
            .raw_json = try allocator.dupe(u8, node.raw_json),
        });
    }
    return try out.toOwnedSlice(allocator);
}

fn freeStringSlice(allocator: std.mem.Allocator, items: [][]u8) void {
    for (items) |item| allocator.free(item);
    allocator.free(items);
}

fn parseInputJsonDocumentsAlloc(allocator: std.mem.Allocator, input_bytes: []const u8) ![][]u8 {
    const trimmed = std.mem.trim(u8, input_bytes, " \t\r\n");
    if (trimmed.len == 0) return error.InvalidArguments;

    if (try parseInputAsWholeJsonAlloc(allocator, trimmed)) |docs| {
        return docs;
    }
    return try parseInputAsJsonLinesAlloc(allocator, trimmed);
}

fn parseInputAsWholeJsonAlloc(allocator: std.mem.Allocator, input_bytes: []const u8) !?[][]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, allocator, input_bytes, .{}) catch return null;
    defer parsed.deinit();

    switch (parsed.value) {
        .object => {
            const rendered = try renderCompactJsonAlloc(allocator, parsed.value);
            errdefer allocator.free(rendered);
            const docs = try allocator.alloc([]u8, 1);
            docs[0] = rendered;
            return docs;
        },
        .array => |arr| {
            var docs = std.ArrayList([]u8){};
            errdefer {
                for (docs.items) |doc| allocator.free(doc);
                docs.deinit(allocator);
            }
            for (arr.items) |item| {
                if (item != .object) return error.InvalidArguments;
                try docs.append(allocator, try renderCompactJsonAlloc(allocator, item));
            }
            return try docs.toOwnedSlice(allocator);
        },
        else => return error.InvalidArguments,
    }
}

fn parseInputAsJsonLinesAlloc(allocator: std.mem.Allocator, input_bytes: []const u8) ![][]u8 {
    var docs = std.ArrayList([]u8){};
    errdefer {
        for (docs.items) |doc| allocator.free(doc);
        docs.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, input_bytes, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (line.len == 0) continue;
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, line, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidArguments;
        try docs.append(allocator, try renderCompactJsonAlloc(allocator, parsed.value));
    }

    if (docs.items.len == 0) return error.InvalidArguments;
    return try docs.toOwnedSlice(allocator);
}

fn renderCompactJsonAlloc(allocator: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, value, .{});
}

fn importNodeDocument(allocator: std.mem.Allocator, state: *SchemaState, raw_doc: []const u8, query: QueryOptions, position: []const u8, added: *usize, updated: *usize) !void {
    const preferred_id = try extractStringFieldAlloc(allocator, raw_doc, "_id");
    defer if (preferred_id) |value| allocator.free(value);

    var existing_idx: ?usize = null;
    var assigned_id: []u8 = undefined;

    if (query.reuse_ids and preferred_id != null and preferred_id.?.len > 0) {
        existing_idx = findNodeIndexById(state.nodes, preferred_id.?);
        assigned_id = try allocator.dupe(u8, preferred_id.?);
    } else {
        assigned_id = try std.fmt.allocPrint(allocator, "{d}", .{state.next_id});
        state.next_id += 1;
    }
    defer allocator.free(assigned_id);

    const existing_node = if (existing_idx) |idx| &state.nodes[idx] else null;
    const normalized = try normalizeNodeJsonForWriteAlloc(allocator, raw_doc, assigned_id, query.source, existing_node);
    defer allocator.free(normalized);
    const new_record = try parseNodeRecordFromRawJson(allocator, normalized);

    if (existing_idx) |idx| {
        state.nodes[idx].deinit(allocator);
        state.nodes[idx] = new_record;
        updated.* += 1;
        return;
    }

    try insertNodeAtPosition(allocator, &state.nodes, new_record, position);
    added.* += 1;
}

fn deleteMatchingNodes(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions, single_only: bool) !usize {
    if (!query.all and !query.all_subscribe and query.ids_csv == null and query.source == null and query.source_tag == null and query.airport_identity == null and query.group == null and query.protocol == null and query.name == null and query.identity == null) {
        return error.InvalidArguments;
    }

    var kept = std.ArrayList(NodeRecord){};
    errdefer kept.deinit(allocator);
    var removed: usize = 0;

    for (state.nodes) |*node| {
        const matched = if (query.all)
            true
        else if (query.all_subscribe)
            std.mem.eql(u8, node.source, "subscribe")
        else
            matchCsv(query.ids_csv orelse "", node.id) and
                matchSource(query.source orelse "", node.source) and
                matchOptionalExact(query.source_tag orelse "", node.source_tag) and
                matchOptionalExact(query.airport_identity orelse "", node.airport_identity) and
                matchOptionalExact(query.group orelse "", node.group) and
                matchOptionalExactCaseFold(query.protocol orelse "", node.protocol) and
                matchOptionalExact(query.identity orelse "", node.identity) and
                matchOptionalSubstringCaseFold(query.name orelse "", node.name);

        if (matched and (!single_only or removed == 0)) {
            removed += 1;
            node.deinit(allocator);
        } else {
            try kept.append(allocator, node.*);
        }
    }

    allocator.free(state.nodes);
    state.nodes = try kept.toOwnedSlice(allocator);
    return removed;
}

fn insertNodeAtPosition(allocator: std.mem.Allocator, nodes: *[]NodeRecord, node: NodeRecord, position: []const u8) !void {
    var list = std.ArrayList(NodeRecord){};
    defer list.deinit(allocator);
    for (nodes.*) |existing| try list.append(allocator, existing);

    if (std.ascii.eqlIgnoreCase(position, "tail")) {
        try list.append(allocator, node);
    } else if (std.ascii.eqlIgnoreCase(position, "head")) {
        try list.insert(allocator, 0, node);
    } else if (std.mem.startsWith(u8, position, "before:")) {
        const target = position["before:".len..];
        const idx = findNodeIndexById(list.items, target) orelse list.items.len;
        try list.insert(allocator, idx, node);
    } else if (std.mem.startsWith(u8, position, "after:")) {
        const target = position["after:".len..];
        const idx = if (findNodeIndexById(list.items, target)) |value| value + 1 else list.items.len;
        try list.insert(allocator, idx, node);
    } else {
        return error.InvalidArguments;
    }

    allocator.free(nodes.*);
    nodes.* = try list.toOwnedSlice(allocator);
}

fn printMutationSummary(command_name: []const u8, added: usize, updated: usize, removed: usize, final_count: usize, dry_run: bool) !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("command: {s}\n", .{command_name});
    try stdout.print("dry_run: {s}\n", .{if (dry_run) "1" else "0"});
    try stdout.print("added: {d}\n", .{added});
    try stdout.print("updated: {d}\n", .{updated});
    try stdout.print("removed: {d}\n", .{removed});
    try stdout.print("final_count: {d}\n", .{final_count});
}

fn reorderNodeRecordsAlloc(allocator: std.mem.Allocator, nodes: []NodeRecord, order_csv: ?[]const u8) ![]NodeRecord {
    if (nodes.len == 0) return try allocator.alloc(NodeRecord, 0);

    const out = try allocator.alloc(NodeRecord, nodes.len);
    errdefer allocator.free(out);
    const used = try allocator.alloc(bool, nodes.len);
    defer allocator.free(used);
    @memset(used, false);

    var out_idx: usize = 0;
    if (order_csv) |csv| {
        var it = std.mem.splitScalar(u8, csv, ',');
        while (it.next()) |raw_id| {
            const wanted = std.mem.trim(u8, raw_id, " \t\r\n");
            if (wanted.len == 0) continue;
            for (nodes, 0..) |node, idx| {
                if (used[idx]) continue;
                if (std.mem.eql(u8, node.id, wanted)) {
                    out[out_idx] = node;
                    used[idx] = true;
                    out_idx += 1;
                    break;
                }
            }
        }
    }

    for (nodes, 0..) |node, idx| {
        if (used[idx]) continue;
        out[out_idx] = node;
        out_idx += 1;
    }

    return out[0..out_idx];
}

fn parseNodeRecord(allocator: std.mem.Allocator, db_key: []const u8, encoded_value: []const u8) !NodeRecord {
    const decoded = try decodeBase64StandardAlloc(allocator, encoded_value);
    errdefer allocator.free(decoded);
    return try parseNodeRecordFromDecodedOwnedJson(allocator, decoded, db_key["fss_node_".len..]);
}

fn parseNodeRecordFromRawJson(allocator: std.mem.Allocator, raw_json: []const u8) !NodeRecord {
    const owned = try allocator.dupe(u8, raw_json);
    errdefer allocator.free(owned);
    return try parseNodeRecordFromDecodedOwnedJson(allocator, owned, "");
}

fn parseNodeRecordFromDecodedOwnedJson(allocator: std.mem.Allocator, owned_json: []u8, fallback_id: []const u8) !NodeRecord {
    var parsed = try std.json.parseFromSlice(NodeJson, allocator, owned_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const value = parsed.value;

    const node_id = value._id orelse fallback_id;
    const protocol_info = protocolInfo(value.type orelse "", value.xray_prot orelse "");
    const endpoint = effectiveEndpoint(value);
    const source = value._source orelse "";
    const group = value.group orelse "";
    const source_tag = deriveSourceTag(group, source);

    return .{
        .id = try allocator.dupe(u8, node_id),
        .type_id = try allocator.dupe(u8, value.type orelse ""),
        .protocol = try allocator.dupe(u8, protocol_info.short),
        .protocol_label = try allocator.dupe(u8, protocol_info.label),
        .name = try allocator.dupe(u8, value.name orelse ""),
        .group = try allocator.dupe(u8, group),
        .source = try allocator.dupe(u8, source),
        .source_tag = try allocator.dupe(u8, source_tag),
        .airport_identity = try allocator.dupe(u8, value._airport_identity orelse ""),
        .source_scope = try allocator.dupe(u8, value._source_scope orelse ""),
        .source_url_hash = try allocator.dupe(u8, value._source_url_hash orelse ""),
        .identity = try allocator.dupe(u8, value._identity orelse ""),
        .identity_primary = try allocator.dupe(u8, value._identity_primary orelse ""),
        .identity_secondary = try allocator.dupe(u8, value._identity_secondary orelse ""),
        .server = try allocator.dupe(u8, endpoint.server),
        .port = try allocator.dupe(u8, endpoint.port),
        .schema = value._schema orelse 0,
        .rev = value._rev orelse 0,
        .created_at = value._created_at orelse 0,
        .updated_at = value._updated_at orelse 0,
        .raw_json = owned_json,
    };
}

fn normalizeNodeJsonForWriteAlloc(allocator: std.mem.Allocator, raw_json: []const u8, assigned_id: []const u8, source_override: ?[]const u8, existing_node: ?*const NodeRecord) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var parsed = try std.json.parseFromSlice(std.json.Value, arena, raw_json, .{});
    const root = &parsed.value;
    if (root.* != .object) return error.InvalidArguments;
    const obj = &root.object;

    const ts = nowMs();
    const name = try jsonGetStringAlloc(arena, obj, "name");
    const group = try jsonGetStringAlloc(arena, obj, "group");
    var source = try jsonGetStringAlloc(arena, obj, "_source");
    if (source_override) |override| source = override;
    if (source.len == 0) source = "user";

    var airport_identity = try jsonGetStringAlloc(arena, obj, "_airport_identity");
    var source_scope = try jsonGetStringAlloc(arena, obj, "_source_scope");
    var source_url_hash = try jsonGetStringAlloc(arena, obj, "_source_url_hash");

    if (std.mem.eql(u8, source, "subscribe")) {
        if ((airport_identity.len == 0 or source_scope.len == 0) and group.len > 0) {
            if (splitGroupSuffix(group)) |parts| {
                if (airport_identity.len == 0) airport_identity = try slugifyAlloc(arena, parts.base, "sub");
                if (source_scope.len == 0) source_scope = try std.fmt.allocPrint(arena, "{s}_{s}", .{ airport_identity, parts.suffix });
                if (source_url_hash.len == 0) source_url_hash = parts.suffix;
            }
        }
        if (airport_identity.len == 0) airport_identity = try slugifyAlloc(arena, if (group.len > 0) group else "sub", "sub");
        if (source_scope.len == 0) {
            source_scope = if (source_url_hash.len > 0)
                try std.fmt.allocPrint(arena, "{s}_{s}", .{ airport_identity, source_url_hash })
            else
                airport_identity;
        }
    } else {
        if (airport_identity.len == 0) airport_identity = "local";
        if (source_scope.len == 0) source_scope = "local";
        if (std.mem.eql(u8, source_scope, "local")) source_url_hash = "";
    }

    const primary_input = try std.fmt.allocPrint(arena, "{s}\x1f{s}", .{ source_scope, name });
    const identity_primary = try hashShellStyleHex8Alloc(allocator, primary_input);
    defer allocator.free(identity_primary);

    const secondary_payload = try renderSecondaryPayloadCanonicalAlloc(arena, root.*);
    const identity_secondary = try hashShellStyleHex8Alloc(allocator, secondary_payload);
    defer allocator.free(identity_secondary);

    try jsonPutString(arena, obj, "_id", assigned_id);
    try jsonPutInteger(obj, "_schema", 2);
    try jsonPutInteger(obj, "_rev", if (existing_node) |node| @as(i64, @intCast(node.rev + 1)) else @as(i64, 1));
    try jsonPutString(arena, obj, "_b64_mode", "raw");
    try jsonPutString(arena, obj, "_source", source);
    try jsonPutString(arena, obj, "_airport_identity", airport_identity);
    try jsonPutString(arena, obj, "_source_scope", source_scope);
    try jsonPutString(arena, obj, "_source_url_hash", source_url_hash);
    try jsonPutString(arena, obj, "_identity_primary", identity_primary);
    try jsonPutString(arena, obj, "_identity_secondary", identity_secondary);
    const identity = try std.fmt.allocPrint(arena, "{s}_{s}", .{ identity_primary, identity_secondary });
    try jsonPutString(arena, obj, "_identity", identity);
    try jsonPutString(arena, obj, "_identity_ver", "1");
    try jsonPutInteger(obj, "_updated_at", @as(i64, @intCast(ts)));
    try jsonPutInteger(obj, "_created_at", if (existing_node) |node| @as(i64, @intCast(node.created_at)) else @as(i64, @intCast(ts)));
    _ = obj.orderedRemove("server_ip");
    _ = obj.orderedRemove("latency");
    _ = obj.orderedRemove("ping");

    return try renderCompactJsonAlloc(allocator, root.*);
}

fn splitGroupSuffix(group: []const u8) ?struct { base: []const u8, suffix: []const u8 } {
    const pos = std.mem.lastIndexOfScalar(u8, group, '_') orelse return null;
    if (pos == 0 or pos + 5 != group.len) return null;
    const suffix = group[pos + 1 ..];
    for (suffix) |ch| {
        if (!std.ascii.isHex(ch)) return null;
    }
    return .{ .base = group[0..pos], .suffix = suffix };
}

fn slugifyAlloc(allocator: std.mem.Allocator, raw: []const u8, fallback: []const u8) ![]const u8 {
    var out = std.ArrayList(u8){};
    errdefer out.deinit(allocator);
    var prev_sep = false;
    for (raw) |ch| {
        const lower = std.ascii.toLower(ch);
        if (std.ascii.isAlphanumeric(lower)) {
            try out.append(allocator, lower);
            prev_sep = false;
        } else if (!prev_sep and out.items.len > 0) {
            try out.append(allocator, '_');
            prev_sep = true;
        }
    }
    while (out.items.len > 0 and out.items[out.items.len - 1] == '_') _ = out.pop();
    if (out.items.len == 0) {
        return try allocator.dupe(u8, fallback);
    }
    return try out.toOwnedSlice(allocator);
}

fn jsonGetStringAlloc(allocator: std.mem.Allocator, obj: *const std.json.ObjectMap, key: []const u8) ![]const u8 {
    const value = obj.get(key) orelse return "";
    return switch (value) {
        .string => try allocator.dupe(u8, value.string),
        .integer => try std.fmt.allocPrint(allocator, "{d}", .{value.integer}),
        .bool => try allocator.dupe(u8, if (value.bool) "1" else "0"),
        else => "",
    };
}

fn jsonPutString(allocator: std.mem.Allocator, obj: *std.json.ObjectMap, key: []const u8, value: []const u8) !void {
    try obj.put(key, .{ .string = try allocator.dupe(u8, value) });
}

fn jsonPutInteger(obj: *std.json.ObjectMap, key: []const u8, value: i64) !void {
    try obj.put(key, .{ .integer = value });
}

fn renderSecondaryPayloadCanonicalAlloc(allocator: std.mem.Allocator, root: std.json.Value) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    try writeCanonicalValue(allocator, out.writer(allocator), root, true, 0);
    return try out.toOwnedSlice(allocator);
}

fn writeCanonicalValue(allocator: std.mem.Allocator, writer: anytype, value: std.json.Value, root_mode: bool, depth: usize) !void {
    switch (value) {
        .null => try writer.writeAll("null"),
        .bool => |b| try writer.writeAll(if (b) "true" else "false"),
        .integer => |v| try writer.print("{}", .{v}),
        .float => |v| try writer.print("{d}", .{v}),
        .number_string => |v| try writer.writeAll(v),
        .string => |v| try writeJsonString(writer, v),
        .array => |arr| {
            try writer.writeByte('[');
            for (arr.items, 0..) |item, idx| {
                if (idx > 0) try writer.writeByte(',');
                try writeCanonicalValue(allocator, writer, item, false, depth + 1);
            }
            try writer.writeByte(']');
        },
        .object => |obj| {
            var keys = std.ArrayList([]const u8){};
            defer keys.deinit(allocator);
            var it = obj.iterator();
            while (it.next()) |entry| {
                if (root_mode and depth == 0 and shouldSkipSecondaryKey(entry.key_ptr.*)) continue;
                try keys.append(allocator, entry.key_ptr.*);
            }
            std.mem.sort([]const u8, keys.items, {}, lessThanString);
            try writer.writeByte('{');
            for (keys.items, 0..) |key, idx| {
                if (idx > 0) try writer.writeByte(',');
                try writeJsonString(writer, key);
                try writer.writeByte(':');
                try writeCanonicalValue(allocator, writer, obj.get(key).?, false, depth + 1);
            }
            try writer.writeByte('}');
        },
    }
}

fn lessThanString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.order(u8, lhs, rhs) == .lt;
}

fn shouldSkipSecondaryKey(key: []const u8) bool {
    return std.mem.eql(u8, key, "name") or
        std.mem.eql(u8, key, "group") or
        std.mem.eql(u8, key, "mode") or
        std.mem.eql(u8, key, "_id") or
        std.mem.eql(u8, key, "_schema") or
        std.mem.eql(u8, key, "_rev") or
        std.mem.eql(u8, key, "_source") or
        std.mem.eql(u8, key, "_updated_at") or
        std.mem.eql(u8, key, "_created_at") or
        std.mem.eql(u8, key, "_migrated_from") or
        std.mem.eql(u8, key, "_b64_mode") or
        std.mem.eql(u8, key, "_airport_identity") or
        std.mem.eql(u8, key, "_source_scope") or
        std.mem.eql(u8, key, "_source_url_hash") or
        std.mem.eql(u8, key, "_identity_primary") or
        std.mem.eql(u8, key, "_identity_secondary") or
        std.mem.eql(u8, key, "_identity") or
        std.mem.eql(u8, key, "_identity_ver") or
        std.mem.eql(u8, key, "_identity_slot") or
        std.mem.eql(u8, key, "server_ip") or
        std.mem.eql(u8, key, "latency") or
        std.mem.eql(u8, key, "ping");
}

fn hashShellStyleHex8Alloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const cksum = std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ "sh", "-c", "printf '%s' \"$1\" | cksum", "sh", input },
        .max_output_bytes = 4096,
    }) catch return try md5Hex8Alloc(allocator, input);
    defer allocator.free(cksum.stdout);
    defer allocator.free(cksum.stderr);
    if (cksum.term == .Exited and cksum.term.Exited == 0) {
        var fields = std.mem.tokenizeAny(u8, cksum.stdout, " \t\r\n");
        if (fields.next()) |crc_dec| {
            const value = std.fmt.parseInt(u32, crc_dec, 10) catch return try md5Hex8Alloc(allocator, input);
            return try std.fmt.allocPrint(allocator, "{x:0>8}", .{value});
        }
    }
    return try md5Hex8Alloc(allocator, input);
}

fn md5Hex8Alloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var digest: [16]u8 = undefined;
    std.crypto.hash.Md5.hash(input, &digest, .{});
    return try std.fmt.allocPrint(allocator, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{ digest[0], digest[1], digest[2], digest[3] });
}

fn extractStringFieldAlloc(allocator: std.mem.Allocator, raw_json: []const u8, key: []const u8) !?[]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    const value = parsed.value.object.get(key) orelse return null;
    return switch (value) {
        .string => try allocator.dupe(u8, value.string),
        .integer => try std.fmt.allocPrint(allocator, "{d}", .{value.integer}),
        else => null,
    };
}

fn encodeBase64StandardAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const size = std.base64.standard.Encoder.calcSize(input.len);
    const out = try allocator.alloc(u8, size);
    _ = std.base64.standard.Encoder.encode(out, input);
    return out;
}

fn freeNodeSlice(allocator: std.mem.Allocator, nodes: []NodeRecord) void {
    for (nodes) |*node| node.deinit(allocator);
    allocator.free(nodes);
}

fn freeStatItems(allocator: std.mem.Allocator, items: *std.ArrayList(StatItem)) void {
    for (items.items) |*item| item.deinit(allocator);
    items.deinit(allocator);
}

fn bumpStatItem(allocator: std.mem.Allocator, items: *std.ArrayList(StatItem), key: []const u8) !void {
    for (items.items) |*item| {
        if (std.mem.eql(u8, item.key, key)) {
            item.count += 1;
            return;
        }
    }
    try items.append(allocator, .{
        .key = try allocator.dupe(u8, key),
        .count = 1,
    });
}

fn filterNodeSliceInPlace(allocator: std.mem.Allocator, nodes: *[]NodeRecord, query: QueryOptions) !void {
    const source_value = query.source orelse "";
    const source_tag_value = query.source_tag orelse "";
    const airport_value = query.airport_identity orelse "";
    const group_value = query.group orelse "";
    const protocol_value = query.protocol orelse "";
    const name_value = query.name orelse "";
    const identity_value = query.identity orelse "";
    const ids_csv_value = query.ids_csv orelse "";

    var kept = std.ArrayList(NodeRecord){};
    errdefer kept.deinit(allocator);

    for (nodes.*) |node| {
        if (!matchCsv(ids_csv_value, node.id)) continue;
        if (!matchSource(source_value, node.source)) continue;
        if (!matchOptionalExact(source_tag_value, node.source_tag)) continue;
        if (!matchOptionalExact(airport_value, node.airport_identity)) continue;
        if (!matchOptionalExact(group_value, node.group)) continue;
        if (!matchOptionalExactCaseFold(protocol_value, node.protocol)) continue;
        if (!matchOptionalExact(identity_value, node.identity)) continue;
        if (!matchOptionalSubstringCaseFold(name_value, node.name)) continue;
        try kept.append(allocator, node);
    }

    allocator.free(nodes.*);
    nodes.* = try kept.toOwnedSlice(allocator);
}

fn matchCsv(csv: []const u8, value: []const u8) bool {
    if (csv.len == 0) return true;
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) continue;
        if (std.mem.eql(u8, item, value)) return true;
    }
    return false;
}

fn matchSource(filter_value: []const u8, source: []const u8) bool {
    if (filter_value.len == 0) return true;
    if (std.ascii.eqlIgnoreCase(filter_value, "subscribe")) {
        return std.mem.eql(u8, source, "subscribe");
    }
    if (std.ascii.eqlIgnoreCase(filter_value, "user")) {
        return !std.mem.eql(u8, source, "subscribe");
    }
    return std.ascii.eqlIgnoreCase(filter_value, source);
}

fn matchOptionalExact(filter_value: []const u8, value: []const u8) bool {
    if (filter_value.len == 0) return true;
    return std.mem.eql(u8, filter_value, value);
}

fn matchOptionalExactCaseFold(filter_value: []const u8, value: []const u8) bool {
    if (filter_value.len == 0) return true;
    return std.ascii.eqlIgnoreCase(filter_value, value);
}

fn matchOptionalSubstringCaseFold(filter_value: []const u8, value: []const u8) bool {
    if (filter_value.len == 0) return true;
    return containsAsciiCaseFold(value, filter_value);
}

fn containsAsciiCaseFold(haystack: []const u8, needle: []const u8) bool {
    if (needle.len == 0) return true;
    if (needle.len > haystack.len) return false;
    var idx: usize = 0;
    while (idx + needle.len <= haystack.len) : (idx += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[idx .. idx + needle.len], needle)) return true;
    }
    return false;
}

fn isNumericNodeKey(key: []const u8) bool {
    if (!std.mem.startsWith(u8, key, "fss_node_")) return false;
    const suffix = key["fss_node_".len..];
    if (suffix.len == 0) return false;
    for (suffix) |ch| {
        if (ch < '0' or ch > '9') return false;
    }
    return true;
}

fn parseSkipdBody(body: []const u8) !SkipdBody {
    const trimmed = std.mem.trimRight(u8, body, "\r\n");
    const first_space = std.mem.indexOfScalar(u8, trimmed, ' ') orelse return error.InvalidSkipdBody;
    const second_space = std.mem.indexOfScalarPos(u8, trimmed, first_space + 1, ' ') orelse return error.InvalidSkipdBody;
    return .{
        .command = trimmed[0..first_space],
        .key = trimmed[first_space + 1 .. second_space],
        .value = trimmed[second_space + 1 ..],
    };
}

fn protocolInfo(type_id: []const u8, xray_prot: []const u8) struct { short: []const u8, label: []const u8 } {
    if (std.mem.eql(u8, type_id, "0")) return .{ .short = "ss", .label = "SS" };
    if (std.mem.eql(u8, type_id, "1")) return .{ .short = "ssr", .label = "SSR" };
    if (std.mem.eql(u8, type_id, "3")) return .{ .short = "vmess", .label = "VMess" };
    if (std.mem.eql(u8, type_id, "4")) {
        if (std.ascii.eqlIgnoreCase(xray_prot, "vmess")) return .{ .short = "vmess", .label = "VMess" };
        if (std.ascii.eqlIgnoreCase(xray_prot, "vless") or xray_prot.len == 0) return .{ .short = "vless", .label = "VLESS" };
        return .{ .short = "xray", .label = "Xray" };
    }
    if (std.mem.eql(u8, type_id, "5")) return .{ .short = "trojan", .label = "Trojan" };
    if (std.mem.eql(u8, type_id, "6")) return .{ .short = "naive", .label = "Naive" };
    if (std.mem.eql(u8, type_id, "7")) return .{ .short = "tuic", .label = "TUIC" };
    if (std.mem.eql(u8, type_id, "8")) return .{ .short = "hysteria2", .label = "Hysteria2" };
    return .{ .short = "unknown", .label = "Unknown" };
}

fn effectiveEndpoint(value: NodeJson) struct { server: []const u8, port: []const u8 } {
    if (value.type) |type_id| {
        if (std.mem.eql(u8, type_id, "6")) {
            return .{ .server = value.naive_server orelse "", .port = value.naive_port orelse "" };
        }
        if (std.mem.eql(u8, type_id, "8")) {
            return .{ .server = value.hy2_server orelse "", .port = value.hy2_port orelse "" };
        }
    }
    return .{ .server = value.server orelse "", .port = value.port orelse "" };
}

fn deriveSourceTag(group: []const u8, source: []const u8) []const u8 {
    if (!std.mem.eql(u8, source, "subscribe")) return "";
    const pos = std.mem.lastIndexOfScalar(u8, group, '_') orelse return "";
    if (pos + 1 >= group.len) return "";
    return group[pos + 1 ..];
}

fn decodeBase64StandardAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const Decoder = std.base64.standard.Decoder;
    const size = try Decoder.calcSizeForSlice(input);
    const out = try allocator.alloc(u8, size);
    errdefer allocator.free(out);
    try Decoder.decode(out, input);
    return out;
}

fn writeListTable(writer: anytype, nodes: []const NodeRecord) !void {
    try writer.writeAll("ID\tPROTO\tSOURCE\tTAG\tAIRPORT\tNAME\n");
    for (nodes) |node| {
        try writer.print("{s}\t{s}\t{s}\t{s}\t{s}\t{s}\n", .{
            node.id,
            node.protocol,
            if (node.source.len > 0) node.source else "user",
            node.source_tag,
            if (node.airport_identity.len > 0) node.airport_identity else "local",
            node.name,
        });
    }
}

fn writeListJson(allocator: std.mem.Allocator, writer: anytype, nodes: []const NodeRecord, jsonl: bool) !void {
    if (!jsonl) try writer.writeAll("[\n");
    for (nodes, 0..) |node, idx| {
        const rendered = try renderNodeViewJsonAlloc(allocator, node);
        defer allocator.free(rendered);
        if (jsonl) {
            try writer.writeAll(rendered);
            try writer.writeAll("\n");
        } else {
            if (idx > 0) try writer.writeAll(",\n");
            try writer.writeAll(rendered);
        }
    }
    if (!jsonl) try writer.writeAll("\n]\n");
}

fn writeCanonicalJson(allocator: std.mem.Allocator, writer: anytype, nodes: []const NodeRecord, jsonl: bool) !void {
    if (!jsonl) try writer.writeAll("[\n");
    for (nodes, 0..) |node, idx| {
        const rendered = try renderCanonicalNodeJsonAlloc(allocator, node);
        defer allocator.free(rendered);
        if (jsonl) {
            try writer.writeAll(rendered);
            try writer.writeAll("\n");
        } else {
            if (idx > 0) try writer.writeAll(",\n");
            try writer.writeAll(rendered);
        }
    }
    if (!jsonl) try writer.writeAll("\n]\n");
}

fn writeStatArray(writer: anytype, items: []const StatItem) !void {
    for (items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.writeAll("{\"key\":");
        try writeJsonString(writer, item.key);
        try writer.print(",\"count\":{d}}}", .{item.count});
    }
}

fn renderNodeViewJsonAlloc(allocator: std.mem.Allocator, node: NodeRecord) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    try writer.writeAll("{");
    try writer.writeAll("\"id\":");
    try writeJsonString(writer, node.id);
    try writer.writeAll(",\"protocol\":");
    try writeJsonString(writer, node.protocol);
    try writer.writeAll(",\"protocol_label\":");
    try writeJsonString(writer, node.protocol_label);
    try writer.writeAll(",\"source\":");
    try writeJsonString(writer, if (node.source.len > 0) node.source else "user");
    try writer.writeAll(",\"source_tag\":");
    try writeJsonString(writer, node.source_tag);
    try writer.writeAll(",\"airport_identity\":");
    try writeJsonString(writer, if (node.airport_identity.len > 0) node.airport_identity else "local");
    try writer.writeAll(",\"group\":");
    try writeJsonString(writer, node.group);
    try writer.writeAll(",\"name\":");
    try writeJsonString(writer, node.name);
    try writer.writeAll(",\"server\":");
    try writeJsonString(writer, node.server);
    try writer.writeAll(",\"port\":");
    try writeJsonString(writer, node.port);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, node.identity);
    try writer.writeAll("}");
    return try out.toOwnedSlice(allocator);
}

fn renderCanonicalNodeJsonAlloc(allocator: std.mem.Allocator, node: NodeRecord) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    try writer.writeAll("{");
    try writer.writeAll("\"id\":");
    try writeJsonString(writer, node.id);
    try writer.writeAll(",\"protocol\":");
    try writeJsonString(writer, node.protocol);
    try writer.writeAll(",\"type\":");
    try writeJsonString(writer, node.type_id);
    try writer.writeAll(",\"name\":");
    try writeJsonString(writer, node.name);
    try writer.writeAll(",\"group\":");
    try writeJsonString(writer, node.group);
    try writer.writeAll(",\"source\":");
    try writeJsonString(writer, if (node.source.len > 0) node.source else "user");
    try writer.writeAll(",\"source_tag\":");
    try writeJsonString(writer, node.source_tag);
    try writer.writeAll(",\"airport_identity\":");
    try writeJsonString(writer, if (node.airport_identity.len > 0) node.airport_identity else "local");
    try writer.writeAll(",\"source_scope\":");
    try writeJsonString(writer, node.source_scope);
    try writer.writeAll(",\"source_url_hash\":");
    try writeJsonString(writer, node.source_url_hash);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, node.identity);
    try writer.writeAll(",\"identity_primary\":");
    try writeJsonString(writer, node.identity_primary);
    try writer.writeAll(",\"identity_secondary\":");
    try writeJsonString(writer, node.identity_secondary);
    try writer.writeAll(",\"server\":");
    try writeJsonString(writer, node.server);
    try writer.writeAll(",\"port\":");
    try writeJsonString(writer, node.port);
    try writer.print(",\"schema\":{d},\"rev\":{d},\"created_at\":{d},\"updated_at\":{d}", .{
        node.schema,
        node.rev,
        node.created_at,
        node.updated_at,
    });
    try writer.writeAll("}");
    return try out.toOwnedSlice(allocator);
}

fn writeJsonString(writer: anytype, input: []const u8) !void {
    try writer.writeByte('"');
    for (input) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try writer.print("\\u{X:0>4}", .{@as(u16, c)});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
    try writer.writeByte('"');
}

fn printUsage(writer: anytype) !void {
    try writer.writeAll(
        "Usage:\n" ++
        "  node-tool list [options]\n" ++
        "  node-tool stat [options]\n" ++
        "  node-tool find [options]\n" ++
        "  node-tool node2json [options]\n" ++
        "  node-tool json2node [options]\n" ++
        "  node-tool add-node [options]\n" ++
        "  node-tool delete-node [options]\n" ++
        "  node-tool delete-nodes [options]\n" ++
        "  node-tool warm-cache [options]\n" ++
        "  node-tool reorder [options]\n" ++
        "  node-tool plan [options]\n" ++
        "  node-tool version\n",
    );
}

fn printCommandUsage(command: Command, writer: anytype) !void {
    switch (command) {
        .list => try writer.writeAll("Usage: node-tool list [--ids 1,2,3] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--protocol type] [--name keyword] [--identity value] [--format table|json|jsonl] [--socket path]\n"),
        .stat => try writer.writeAll("Usage: node-tool stat [--ids 1,2,3] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--protocol type] [--name keyword] [--identity value] [--format text|json] [--socket path]\n"),
        .find => try writer.writeAll("Usage: node-tool find [--name keyword] [--identity value] [--source-tag tag] [--airport-identity value] [--protocol type] [--format table|json|jsonl] [--socket path]\n"),
        .node2json => try writer.writeAll("Usage: node-tool node2json [--schema2] [--ids 1,2,3] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--name keyword] [--identity value] [--format json|jsonl] [--canonical] [--socket path]\n"),
        .json2node => try writer.writeAll("Usage: node-tool json2node --input nodes.jsonl [--mode append|replace] [--reuse-ids] [--dry-run]\n"),
        .add_node => try writer.writeAll("Usage: node-tool add-node [--input node.json | --stdin] [--position tail|head|before:<id>|after:<id>] [--source user|subscribe]\n"),
        .delete_node => try writer.writeAll("Usage: node-tool delete-node [--id 23 | --identity xxx_yyy] [--dry-run]\n"),
        .delete_nodes => try writer.writeAll("Usage: node-tool delete-nodes [--ids 1,2,3 | --source-tag tag | --source user | --group name | --all-subscribe | --all] [--dry-run]\n"),
        .warm_cache => try writer.writeAll("Usage: node-tool warm-cache [--env] [--json] [--direct-domains] [--ids 1,2,3]\n"),
        .reorder => try writer.writeAll("Usage: node-tool reorder [--ids 5,3,1] [--source-tag tag --sort name] [--group user --sort created]\n"),
        .plan => try writer.writeAll("Usage: node-tool plan --input nodes.jsonl\n"),
        .version => try writer.writeAll("Usage: node-tool version\n"),
    }
}

test "parse version command" {
    const args = [_][]const u8{ "node-tool", "version" };
    const options = try parseArgs(&args);
    try std.testing.expect(options.command == .version);
}

test "parse skipd list body" {
    const body = "list fss_node_order 1,2,3\n";
    const parsed = try parseSkipdBody(body);
    try std.testing.expect(std.mem.eql(u8, parsed.command, "list"));
    try std.testing.expect(std.mem.eql(u8, parsed.key, "fss_node_order"));
    try std.testing.expect(std.mem.eql(u8, parsed.value, "1,2,3"));
}
