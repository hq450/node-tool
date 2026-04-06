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

const Options = struct {
    command: Command,
    query: QueryOptions = .{},
};

const QueryOptions = struct {
    ids_csv: ?[]const u8 = null,
    source: ?[]const u8 = null,
    source_tag: ?[]const u8 = null,
    airport_identity: ?[]const u8 = null,
    protocol: ?[]const u8 = null,
    name: ?[]const u8 = null,
    identity: ?[]const u8 = null,
    format: ?[]const u8 = null,
    canonical: bool = false,
    schema2: bool = false,
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
        .json2node => try runStub("json2node", options.query),
        .add_node => try runStub("add-node", options.query),
        .delete_node => try runStub("delete-node", options.query),
        .delete_nodes => try runStub("delete-nodes", options.query),
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
        } else if (std.mem.eql(u8, arg, "--canonical")) {
            query.canonical = true;
        } else if (std.mem.eql(u8, arg, "--schema2")) {
            query.schema2 = true;
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

    var parsed = try std.json.parseFromSlice(NodeJson, allocator, decoded, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const value = parsed.value;

    const id_from_key = db_key["fss_node_".len..];
    const node_id = value._id orelse id_from_key;
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
        .raw_json = decoded,
    };
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
