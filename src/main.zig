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
    find_duplicates,
    export_sources,
    prune_export_sources,
    webtest_groups,
    node2json,
    sync_source,
    json2node,
    add_node,
    delete_node,
    delete_nodes,
    dedupe,
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
    shell,
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
    sort: ?[]const u8 = null,
    input: ?[]const u8 = null,
    ids_file: ?[]const u8 = null,
    output_dir: ?[]const u8 = null,
    meta_path: ?[]const u8 = null,
    all_jsonl_path: ?[]const u8 = null,
    active_source_tags_path: ?[]const u8 = null,
    normalized_output: ?[]const u8 = null,
    plan_output: ?[]const u8 = null,
    plan_format: ?[]const u8 = null,
    position: ?[]const u8 = null,
    mode: ?[]const u8 = null,
    canonical: bool = false,
    schema2: bool = false,
    reuse_ids: bool = false,
    dry_run: bool = false,
    stdin_input: bool = false,
    all: bool = false,
    all_subscribe: bool = false,
    warm_env: bool = false,
    warm_json: bool = false,
    warm_direct_domains: bool = false,
    warm_webtest: bool = false,
    duplicate_match: ?[]const u8 = null,
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

const SourceExportBucket = struct {
    source: []u8,
    key: []u8,
    label: []u8,
    path: []u8,
    count: usize,
    content: std.ArrayList(u8),

    fn deinit(self: *SourceExportBucket, allocator: std.mem.Allocator) void {
        allocator.free(self.source);
        allocator.free(self.key);
        allocator.free(self.label);
        allocator.free(self.path);
        self.content.deinit(allocator);
    }
};

const WebtestGroupBucket = struct {
    tag: []u8,
    path: []u8,
    content: std.ArrayList(u8),

    fn deinit(self: *WebtestGroupBucket, allocator: std.mem.Allocator) void {
        allocator.free(self.tag);
        allocator.free(self.path);
        self.content.deinit(allocator);
    }
};

const WebtestRuntimeConfig = struct {
    linux_ver: []u8,
    tfo: []u8,
    resolv_mode: []u8,
    resolver: []u8,

    fn deinit(self: *WebtestRuntimeConfig, allocator: std.mem.Allocator) void {
        allocator.free(self.linux_ver);
        allocator.free(self.tfo);
        allocator.free(self.resolv_mode);
        allocator.free(self.resolver);
    }
};

const PlanItem = struct {
    id: []u8,
    name: []u8,
    protocol: []u8,
    identity: []u8,
    changes: std.ArrayList(PlanFieldChange),

    fn deinit(self: *PlanItem, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.name);
        allocator.free(self.protocol);
        allocator.free(self.identity);
        for (self.changes.items) |*change| change.deinit(allocator);
        self.changes.deinit(allocator);
    }
};

const tracked_plan_fields = [_][]const u8{
    "name",
    "group",
    "server",
    "port",
    "type",
    "xray_prot",
    "method",
    "ss_obfs",
    "xray_network",
    "xray_network_security",
    "xray_network_security_sni",
    "xray_publickey",
    "xray_shortid",
    "trojan_uuid",
    "trojan_sni",
    "naive_server",
    "naive_port",
    "hy2_server",
    "hy2_port",
    "hy2_sni",
    "hy2_obfs",
    "hy2_up",
    "hy2_dl",
};

const PlanFieldChange = struct {
    field: []u8,
    old_value: []u8,
    new_value: []u8,

    fn deinit(self: *PlanFieldChange, allocator: std.mem.Allocator) void {
        allocator.free(self.field);
        allocator.free(self.old_value);
        allocator.free(self.new_value);
    }
};

const ReconcileMapItem = struct {
    match_reason: []u8,
    old_id: []u8,
    old_identity: []u8,
    old_name: []u8,
    new_id: []u8,
    new_identity: []u8,
    new_name: []u8,

    fn deinit(self: *ReconcileMapItem, allocator: std.mem.Allocator) void {
        allocator.free(self.match_reason);
        allocator.free(self.old_id);
        allocator.free(self.old_identity);
        allocator.free(self.old_name);
        allocator.free(self.new_id);
        allocator.free(self.new_identity);
        allocator.free(self.new_name);
    }
};

const PlanSummary = struct {
    added: std.ArrayList(PlanItem),
    updated: std.ArrayList(PlanItem),
    removed: std.ArrayList(PlanItem),
    moved: std.ArrayList(PlanItem),
    mappings: std.ArrayList(ReconcileMapItem),
    unmapped: std.ArrayList(ReconcileMapItem),
    current_before_id: []u8,
    current_before_identity: []u8,
    current_after_id: []u8,
    current_after_identity: []u8,
    failover_before_id: []u8,
    failover_before_identity: []u8,
    failover_after_id: []u8,
    failover_after_identity: []u8,
    final_order: []u8,
    final_count: usize,
    order_changed: bool,

    fn deinit(self: *PlanSummary, allocator: std.mem.Allocator) void {
        for (self.added.items) |*item| item.deinit(allocator);
        self.added.deinit(allocator);
        for (self.updated.items) |*item| item.deinit(allocator);
        self.updated.deinit(allocator);
        for (self.removed.items) |*item| item.deinit(allocator);
        self.removed.deinit(allocator);
        for (self.moved.items) |*item| item.deinit(allocator);
        self.moved.deinit(allocator);
        for (self.mappings.items) |*item| item.deinit(allocator);
        self.mappings.deinit(allocator);
        for (self.unmapped.items) |*item| item.deinit(allocator);
        self.unmapped.deinit(allocator);
        allocator.free(self.current_before_id);
        allocator.free(self.current_before_identity);
        allocator.free(self.current_after_id);
        allocator.free(self.current_after_identity);
        allocator.free(self.failover_before_id);
        allocator.free(self.failover_before_identity);
        allocator.free(self.failover_after_id);
        allocator.free(self.failover_after_identity);
        allocator.free(self.final_order);
    }
};

const ImportHints = struct {
    preferred_id: ?[]u8 = null,
    identity: ?[]u8 = null,
    identity_primary: ?[]u8 = null,
    identity_secondary: ?[]u8 = null,
    source_scope: ?[]u8 = null,

    fn deinit(self: *ImportHints, allocator: std.mem.Allocator) void {
        if (self.preferred_id) |value| allocator.free(value);
        if (self.identity) |value| allocator.free(value);
        if (self.identity_primary) |value| allocator.free(value);
        if (self.identity_secondary) |value| allocator.free(value);
        if (self.source_scope) |value| allocator.free(value);
    }
};

const ImportResult = enum {
    added,
    updated,
    skipped,
};

const DuplicateKind = enum {
    identity,
    config,
};

const DuplicateItem = struct {
    kind: DuplicateKind,
    keep_id: []u8,
    keep_name: []u8,
    remove_id: []u8,
    remove_name: []u8,
    signature: []u8,

    fn deinit(self: *DuplicateItem, allocator: std.mem.Allocator) void {
        allocator.free(self.keep_id);
        allocator.free(self.keep_name);
        allocator.free(self.remove_id);
        allocator.free(self.remove_name);
        allocator.free(self.signature);
    }
};

const DuplicateSummary = struct {
    items: std.ArrayList(DuplicateItem),

    fn deinit(self: *DuplicateSummary, allocator: std.mem.Allocator) void {
        for (self.items.items) |*item| item.deinit(allocator);
        self.items.deinit(allocator);
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
            error.NodeIdConflict => {
                stderr.print("error: explicit _id conflicts with an existing node; use --reuse-ids if you intend to update it\n", .{}) catch {};
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
        .prune_export_sources => {
            try runPruneExportSources(allocator, options.query);
        },
        .list, .stat, .find, .export_sources, .webtest_groups, .node2json => {
            var nodes = try loadSchema2Nodes(allocator, options.query.socket_path);
            defer freeNodeSlice(allocator, nodes);
            try filterNodeSliceInPlace(allocator, &nodes, options.query);
            if (options.query.ids_file) |ids_file| {
                try filterNodeSliceByIdsFileInPlace(allocator, &nodes, ids_file);
            }

            switch (options.command) {
                .list => try runList(allocator, nodes, options.query),
                .stat => try runStat(allocator, nodes, options.query),
                .find => try runFind(allocator, nodes, options.query),
                .export_sources => try runExportSources(allocator, nodes, options.query),
                .webtest_groups => try runWebtestGroups(allocator, nodes, options.query),
                .node2json => try runNode2Json(allocator, nodes, options.query),
                else => unreachable,
            }
        },
        .find_duplicates => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            try runFindDuplicates(allocator, &state, options.query);
        },
        .sync_source, .json2node, .add_node, .delete_node, .delete_nodes => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            switch (options.command) {
                .sync_source => try runSyncSource(allocator, &state, options.query),
                .json2node => try runJson2Node(allocator, &state, options.query),
                .add_node => try runAddNode(allocator, &state, options.query),
                .delete_node => try runDeleteNode(allocator, &state, options.query),
                .delete_nodes => try runDeleteNodes(allocator, &state, options.query),
                else => unreachable,
            }
        },
        .dedupe => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            try runDedupe(allocator, &state, options.query);
        },
        .reorder, .plan => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            switch (options.command) {
                .reorder => try runReorder(allocator, &state, options.query),
                .plan => try runPlan(allocator, &state, options.query),
                else => unreachable,
            }
        },
        .warm_cache => {
            var state = try loadSchema2State(allocator, options.query.socket_path);
            defer state.deinit(allocator);
            try runWarmCache(allocator, &state, options.query);
        },
    }
}

fn parseArgs(args: []const []const u8) !Options {
    if (args.len < 2) return error.InvalidArguments;

    const command = blk: {
        if (std.mem.eql(u8, args[1], "-h") or std.mem.eql(u8, args[1], "--help")) return error.HelpRequested;
        if (std.mem.eql(u8, args[1], "list")) break :blk Command.list;
        if (std.mem.eql(u8, args[1], "stat")) break :blk Command.stat;
        if (std.mem.eql(u8, args[1], "find")) break :blk Command.find;
        if (std.mem.eql(u8, args[1], "find-duplicates")) break :blk Command.find_duplicates;
        if (std.mem.eql(u8, args[1], "export-sources")) break :blk Command.export_sources;
        if (std.mem.eql(u8, args[1], "prune-export-sources")) break :blk Command.prune_export_sources;
        if (std.mem.eql(u8, args[1], "webtest-groups")) break :blk Command.webtest_groups;
        if (std.mem.eql(u8, args[1], "node2json")) break :blk Command.node2json;
        if (std.mem.eql(u8, args[1], "sync-source")) break :blk Command.sync_source;
        if (std.mem.eql(u8, args[1], "json2node")) break :blk Command.json2node;
        if (std.mem.eql(u8, args[1], "add-node")) break :blk Command.add_node;
        if (std.mem.eql(u8, args[1], "delete-node")) break :blk Command.delete_node;
        if (std.mem.eql(u8, args[1], "delete-nodes")) break :blk Command.delete_nodes;
        if (std.mem.eql(u8, args[1], "dedupe")) break :blk Command.dedupe;
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
        } else if (std.mem.eql(u8, arg, "--sort")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.sort = args[i];
        } else if (std.mem.eql(u8, arg, "--input")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.input = args[i];
        } else if (std.mem.eql(u8, arg, "--ids-file")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.ids_file = args[i];
        } else if (std.mem.eql(u8, arg, "--output-dir")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.output_dir = args[i];
        } else if (std.mem.eql(u8, arg, "--meta")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.meta_path = args[i];
        } else if (std.mem.eql(u8, arg, "--all-jsonl")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.all_jsonl_path = args[i];
        } else if (std.mem.eql(u8, arg, "--active-source-tags")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.active_source_tags_path = args[i];
        } else if (std.mem.eql(u8, arg, "--normalized-output")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.normalized_output = args[i];
        } else if (std.mem.eql(u8, arg, "--plan-output")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.plan_output = args[i];
        } else if (std.mem.eql(u8, arg, "--plan-format")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.plan_format = args[i];
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
        } else if (std.mem.eql(u8, arg, "--env")) {
            query.warm_env = true;
        } else if (std.mem.eql(u8, arg, "--json")) {
            query.warm_json = true;
        } else if (std.mem.eql(u8, arg, "--direct-domains")) {
            query.warm_direct_domains = true;
        } else if (std.mem.eql(u8, arg, "--webtest")) {
            query.warm_webtest = true;
        } else if (std.mem.eql(u8, arg, "--match")) {
            i += 1;
            if (i >= args.len) return error.InvalidArguments;
            query.duplicate_match = args[i];
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

fn runFindDuplicates(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    var dupes = try findDuplicates(allocator, state.nodes, query.duplicate_match);
    defer dupes.deinit(allocator);

    const format = query.format orelse "text";
    if (std.ascii.eqlIgnoreCase(format, "json")) {
        try writeDuplicateJson(allocator, std.fs.File.stdout().deprecatedWriter(), &dupes);
    } else if (std.ascii.eqlIgnoreCase(format, "shell")) {
        try writeDuplicateShell(std.fs.File.stdout().deprecatedWriter(), &dupes);
    } else {
        try writeDuplicateText(std.fs.File.stdout().deprecatedWriter(), &dupes);
    }
}

fn runDedupe(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    var dupes = try findDuplicates(allocator, state.nodes, query.duplicate_match);
    defer dupes.deinit(allocator);

    var removed: usize = 0;
    for (dupes.items.items) |dupe| {
        const count = try deleteByExactId(allocator, state, dupe.remove_id);
        removed += count;
    }

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("dedupe", 0, 0, removed, 0, state.nodes.len, query.dry_run);
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

fn runExportSources(allocator: std.mem.Allocator, nodes: []const NodeRecord, query: QueryOptions) !void {
    const output_dir = query.output_dir orelse return error.InvalidArguments;
    const meta_path = query.meta_path orelse return error.InvalidArguments;
    const format = try resolveExportSourcesFormat(query.format);

    std.fs.cwd().makePath(output_dir) catch |err| {
        if (std.fs.cwd().openDir(output_dir, .{})) |opened_dir| {
            var dir = opened_dir;
            dir.close();
        } else |_| {
            return err;
        }
    };

    var buckets = std.ArrayList(SourceExportBucket){};
    defer {
        for (buckets.items) |*bucket| bucket.deinit(allocator);
        buckets.deinit(allocator);
    }
    var all_jsonl = std.ArrayList(u8){};
    defer all_jsonl.deinit(allocator);

    var next_sub_idx: usize = 0;
    for (nodes) |node| {
        const bucket_idx = try findOrCreateSourceBucket(allocator, &buckets, output_dir, node, &next_sub_idx);
        var bucket = &buckets.items[bucket_idx];
        try bucket.content.appendSlice(allocator, node.raw_json);
        try bucket.content.append(allocator, '\n');
        bucket.count += 1;

        try all_jsonl.appendSlice(allocator, node.raw_json);
        try all_jsonl.append(allocator, '\n');
    }

    if (query.all_jsonl_path) |all_jsonl_path| {
        try writeFileAtomic(all_jsonl_path, all_jsonl.items);
    }
    try writeSourceBuckets(allocator, &buckets);
    try writeSourceMetaFile(allocator, meta_path, buckets.items);

    switch (format) {
        .text => try writeSourceExportText(std.fs.File.stdout().deprecatedWriter(), buckets.items, nodes.len),
        .shell => try writeSourceExportShell(std.fs.File.stdout().deprecatedWriter(), buckets.items, nodes.len),
        .json => try writeSourceExportJson(allocator, std.fs.File.stdout().deprecatedWriter(), buckets.items, nodes.len),
        else => unreachable,
    }
}

fn runWebtestGroups(allocator: std.mem.Allocator, nodes: []const NodeRecord, query: QueryOptions) !void {
    const output_dir = query.output_dir orelse return error.InvalidArguments;
    try ensureDirExistsCompat(output_dir);

    var groups = std.ArrayList(WebtestGroupBucket){};
    defer {
        for (groups.items) |*group| group.deinit(allocator);
        groups.deinit(allocator);
    }
    var xray_group = std.ArrayList(u8){};
    defer xray_group.deinit(allocator);
    var nodes_file_name = std.ArrayList(u8){};
    defer nodes_file_name.deinit(allocator);

    for (nodes) |node| {
        const tag = try webtestGroupTagAlloc(allocator, node);
        defer allocator.free(tag);
        if (tag.len == 0) continue;
        const idx = try findOrCreateWebtestGroup(allocator, &groups, output_dir, tag);
        try groups.items[idx].content.appendSlice(allocator, node.id);
        try groups.items[idx].content.append(allocator, '\n');
        if (isWebtestXrayLikeTag(tag)) {
            try xray_group.appendSlice(allocator, node.id);
            try xray_group.append(allocator, '\n');
        }
    }

    for (groups.items) |group| {
        try writeFileAtomic(group.path, group.content.items);
        try nodes_file_name.appendSlice(allocator, group.path);
        try nodes_file_name.append(allocator, '\n');
    }
    if (xray_group.items.len > 0) {
        const xray_path = try std.fmt.allocPrint(allocator, "{s}/wt_xray_group.txt", .{output_dir});
        defer allocator.free(xray_path);
        try writeFileAtomic(xray_path, xray_group.items);
    }
    const names_path = try std.fmt.allocPrint(allocator, "{s}/nodes_file_name.txt", .{output_dir});
    defer allocator.free(names_path);
    try writeFileAtomic(names_path, nodes_file_name.items);

    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("command: webtest-groups\n", .{});
    try stdout.print("groups: {d}\n", .{groups.items.len});
}

fn runPruneExportSources(allocator: std.mem.Allocator, query: QueryOptions) !void {
    const meta_path = query.meta_path orelse return error.InvalidArguments;
    const active_path = query.active_source_tags_path orelse return error.InvalidArguments;
    const format = try resolveExportSourcesFormat(query.format);

    const meta_bytes = try readInputCompatAlloc(allocator, meta_path, false, max_skipd_frame);
    defer allocator.free(meta_bytes);
    const active_bytes = try readInputCompatAlloc(allocator, active_path, false, max_skipd_frame);
    defer allocator.free(active_bytes);

    var active_tags = std.ArrayList([]u8){};
    defer freeStringSlice(allocator, active_tags.items);
    var active_iter = std.mem.splitScalar(u8, active_bytes, '\n');
    while (active_iter.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0) continue;
        try active_tags.append(allocator, try allocator.dupe(u8, line));
    }

    var kept_meta = std.ArrayList(u8){};
    defer kept_meta.deinit(allocator);
    var removed = std.ArrayList(SourceExportBucket){};
    defer {
        for (removed.items) |*item| item.deinit(allocator);
        removed.deinit(allocator);
    }
    var kept_count: usize = 0;

    var line_iter = std.mem.splitScalar(u8, meta_bytes, '\n');
    while (line_iter.next()) |raw_line| {
        const line = std.mem.trimRight(u8, raw_line, "\r");
        if (line.len == 0) continue;
        var fields = std.mem.splitScalar(u8, line, '\t');
        const path = fields.next() orelse continue;
        const count_raw = fields.next() orelse "0";
        const key = fields.next() orelse "";
        const label = fields.next() orelse "";
        const count = std.fmt.parseInt(usize, count_raw, 10) catch 0;

        if (key.len == 0 or std.mem.eql(u8, key, "user") or std.mem.eql(u8, key, "null") or containsSliceOwned(active_tags.items, key)) {
            try kept_meta.appendSlice(allocator, line);
            try kept_meta.append(allocator, '\n');
            kept_count += 1;
            continue;
        }

        std.fs.cwd().deleteFile(path) catch {};
        try removed.append(allocator, .{
            .source = try allocator.dupe(u8, "subscribe"),
            .key = try allocator.dupe(u8, key),
            .label = try allocator.dupe(u8, label),
            .path = try allocator.dupe(u8, path),
            .count = count,
            .content = std.ArrayList(u8){},
        });
    }

    try writeFileAtomic(meta_path, kept_meta.items);

    switch (format) {
        .text => try writePruneExportText(std.fs.File.stdout().deprecatedWriter(), kept_count, removed.items),
        .shell => try writePruneExportShell(std.fs.File.stdout().deprecatedWriter(), kept_count, removed.items),
        .json => try writePruneExportJson(allocator, std.fs.File.stdout().deprecatedWriter(), kept_count, removed.items),
        else => unreachable,
    }
}

fn runJson2Node(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);

    var old_state = try cloneSchemaState(allocator, state.*);
    defer old_state.deinit(allocator);
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const mode = resolveMutationMode(query.mode);
    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);

    var added: usize = 0;
    var updated: usize = 0;
    var skipped: usize = 0;
    if (mode == .replace) {
        freeNodeSlice(allocator, state.nodes);
        state.nodes = try allocator.alloc(NodeRecord, 0);
    }

    for (docs) |raw_doc| {
        const result = try importNodeDocument(allocator, state, raw_doc, query, "tail", if (mode == .replace) old_nodes else null);
        switch (result) {
            .added => added += 1,
            .updated => updated += 1,
            .skipped => skipped += 1,
        }
    }

    try reconcileReferenceState(allocator, state);
    var plan = try buildPlanSummary(allocator, &old_state, old_nodes, state, state.nodes);
    defer plan.deinit(allocator);

    if (query.normalized_output) |normalized_output| {
        try writeRawJsonlToPath(normalized_output, state.nodes);
    }
    if (query.plan_output) |plan_output| {
        try writePlanToPath(allocator, plan_output, query.plan_format orelse "shell", &plan);
    }

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("json2node", plan.added.items.len, plan.updated.items.len, plan.removed.items.len, skipped, state.nodes.len, query.dry_run);
}

fn runSyncSource(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const target_tag = query.source_tag orelse return error.InvalidArguments;
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);

    var old_state = try cloneSchemaState(allocator, state.*);
    defer old_state.deinit(allocator);
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);

    var old_source_nodes = std.ArrayList(NodeRecord){};
    defer {
        for (old_source_nodes.items) |*node| node.deinit(allocator);
        old_source_nodes.deinit(allocator);
    }
    for (old_nodes) |node| {
        if (!std.mem.eql(u8, node.source_tag, target_tag)) continue;
        try old_source_nodes.append(allocator, .{
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

    var source_state = SchemaState{
        .nodes = try allocator.alloc(NodeRecord, 0),
        .current_id = try allocator.dupe(u8, ""),
        .current_identity = try allocator.dupe(u8, ""),
        .failover_id = try allocator.dupe(u8, ""),
        .failover_identity = try allocator.dupe(u8, ""),
        .next_id = state.next_id,
    };
    defer source_state.deinit(allocator);

    var skipped: usize = 0;
    for (docs) |raw_doc| {
        const result = try importNodeDocument(allocator, &source_state, raw_doc, query, "tail", old_source_nodes.items);
        if (result == .skipped) skipped += 1;
    }
    if (query.normalized_output) |normalized_output| {
        try writeRawJsonlToPath(normalized_output, source_state.nodes);
    }

    var final_nodes = std.ArrayList(NodeRecord){};
    defer final_nodes.deinit(allocator);
    var inserted = false;
    var saw_target = false;
    for (state.nodes) |*node| {
        if (std.mem.eql(u8, node.source_tag, target_tag)) {
            saw_target = true;
            if (!inserted) {
                for (source_state.nodes) |new_node| {
                    try final_nodes.append(allocator, new_node);
                }
                allocator.free(source_state.nodes);
                source_state.nodes = try allocator.alloc(NodeRecord, 0);
                inserted = true;
            }
            node.deinit(allocator);
            continue;
        }
        try final_nodes.append(allocator, node.*);
    }
    if (!saw_target) {
        for (source_state.nodes) |new_node| {
            try final_nodes.append(allocator, new_node);
        }
        allocator.free(source_state.nodes);
        source_state.nodes = try allocator.alloc(NodeRecord, 0);
    }
    allocator.free(state.nodes);
    state.nodes = try final_nodes.toOwnedSlice(allocator);
    state.next_id = source_state.next_id;

    try reconcileReferenceState(allocator, state);
    var plan = try buildPlanSummary(allocator, state, old_nodes, state, state.nodes);
    defer plan.deinit(allocator);

    if (query.plan_output) |plan_output| {
        try writePlanToPath(allocator, plan_output, query.plan_format orelse "shell", &plan);
    }
    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("sync-source", plan.added.items.len, plan.updated.items.len, plan.removed.items.len, skipped, state.nodes.len, query.dry_run);
}

fn runAddNode(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);
    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);
    if (docs.len != 1) return error.InvalidArguments;

    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const position = query.position orelse "tail";
    var skipped: usize = 0;
    var added: usize = 0;
    var updated: usize = 0;
    const result = try importNodeDocument(allocator, state, docs[0], query, position, null);
    switch (result) {
        .added => added += 1,
        .updated => updated += 1,
        .skipped => skipped += 1,
    }

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("add-node", added, updated, 0, skipped, state.nodes.len, query.dry_run);
}

fn runDeleteNode(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const removed = try deleteMatchingNodes(allocator, state, query, true);
    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("delete-node", 0, 0, removed, 0, state.nodes.len, query.dry_run);
}

fn runDeleteNodes(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    const removed = try deleteMatchingNodes(allocator, state, query, false);
    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }
    try printMutationSummary("delete-nodes", 0, 0, removed, 0, state.nodes.len, query.dry_run);
}

fn runReorder(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);

    if (query.ids_csv) |ids_csv| {
        try reorderByIds(allocator, &state.nodes, ids_csv);
    } else if (query.sort) |sort_key| {
        try reorderBySort(allocator, &state.nodes, query, sort_key);
    } else {
        return error.InvalidArguments;
    }

    if (!query.dry_run) {
        try writeSchema2State(allocator, query.socket_path, old_nodes, state);
    }

    const order_csv = try joinNodeOrderCsvAlloc(allocator, state.nodes);
    defer allocator.free(order_csv);
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("command: reorder\n", .{});
    try stdout.print("dry_run: {s}\n", .{if (query.dry_run) "1" else "0"});
    try stdout.print("final_count: {d}\n", .{state.nodes.len});
    try stdout.print("order: {s}\n", .{order_csv});
}

fn runPlan(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    const input_bytes = try readInputCompatAlloc(allocator, query.input, query.stdin_input or query.input == null, max_skipd_frame);
    defer allocator.free(input_bytes);

    const old_nodes = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, old_nodes);
    var draft = try cloneSchemaState(allocator, state.*);
    defer draft.deinit(allocator);

    const docs = try parseInputJsonDocumentsAlloc(allocator, input_bytes);
    defer freeStringSlice(allocator, docs);

    const mode = resolveMutationMode(query.mode);
    var added: usize = 0;
    var updated: usize = 0;
    var removed: usize = 0;

    if (mode == .replace) {
        removed = draft.nodes.len;
        freeNodeSlice(allocator, draft.nodes);
        draft.nodes = try allocator.alloc(NodeRecord, 0);
    }
    for (docs) |raw_doc| {
        const result = try importNodeDocument(allocator, &draft, raw_doc, query, "tail", if (mode == .replace) old_nodes else null);
        switch (result) {
            .added => added += 1,
            .updated => updated += 1,
            .skipped => {},
        }
    }
    try reconcileReferenceState(allocator, &draft);

    var plan = try buildPlanSummary(allocator, state, old_nodes, &draft, draft.nodes);
    defer plan.deinit(allocator);

    const format = query.format orelse "json";
    if (std.ascii.eqlIgnoreCase(format, "text")) {
        try writePlanText(std.fs.File.stdout().deprecatedWriter(), &plan);
    } else if (std.ascii.eqlIgnoreCase(format, "shell")) {
        try writePlanShell(std.fs.File.stdout().deprecatedWriter(), &plan);
    } else {
        try writePlanJson(allocator, std.fs.File.stdout().deprecatedWriter(), &plan);
    }
}

fn runWarmCache(allocator: std.mem.Allocator, state: *SchemaState, query: QueryOptions) !void {
    var targets = try cloneNodeSlice(allocator, state.nodes);
    defer freeNodeSlice(allocator, targets);
    try filterNodeSliceInPlace(allocator, &targets, query);
    if (query.ids_file) |ids_file| {
        try filterNodeSliceByIdsFileInPlace(allocator, &targets, ids_file);
    }

    const no_specific = !query.warm_env and !query.warm_json and !query.warm_direct_domains and !query.warm_webtest;
    const do_env = query.warm_env or no_specific;
    const do_json = query.warm_json or no_specific;
    const do_direct = query.warm_direct_domains or no_specific;
    const do_webtest = query.warm_webtest or no_specific;

    var json_count: usize = 0;
    var env_count: usize = 0;
    var direct_count: usize = 0;
    var webtest_count: usize = 0;

    if (do_json) json_count = try warmJsonCache(allocator, targets);
    if (do_env) env_count = try warmEnvCache(allocator, targets);
    if (do_direct) direct_count = try warmDirectDomainsCache(allocator, state.nodes);
    if (do_webtest) {
        if (!do_json) json_count = try warmJsonCache(allocator, targets);
        if (!do_env) env_count = try warmEnvCache(allocator, targets);
        webtest_count = try warmWebtestArtifacts(allocator, query.socket_path, targets);
    }

    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("command: warm-cache\n", .{});
    try stdout.print("json: {d}\n", .{json_count});
    try stdout.print("env: {d}\n", .{env_count});
    try stdout.print("direct_domains: {d}\n", .{direct_count});
    try stdout.print("webtest: {d}\n", .{webtest_count});
}

fn filterNodeSliceByIdsFileInPlace(allocator: std.mem.Allocator, nodes: *[]NodeRecord, ids_file: []const u8) !void {
    const content = try readFileAllocCompat(allocator, ids_file, max_skipd_frame);
    defer allocator.free(content);

    var ids = std.ArrayList([]const u8){};
    defer ids.deinit(allocator);
    var it = std.mem.splitScalar(u8, content, '\n');
    while (it.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r\n");
        if (line.len == 0) continue;
        try ids.append(allocator, line);
    }

    var kept = std.ArrayList(NodeRecord){};
    errdefer kept.deinit(allocator);
    for (nodes.*) |node| {
        if (!containsSliceConst(ids.items, node.id)) continue;
        try kept.append(allocator, node);
    }
    allocator.free(nodes.*);
    nodes.* = try kept.toOwnedSlice(allocator);
}

fn writePlanToPath(allocator: std.mem.Allocator, path: []const u8, format: []const u8, plan: *PlanSummary) !void {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    if (std.ascii.eqlIgnoreCase(format, "text")) {
        try writePlanText(writer, plan);
    } else if (std.ascii.eqlIgnoreCase(format, "shell")) {
        try writePlanShell(writer, plan);
    } else {
        try writePlanJson(allocator, writer, plan);
    }
    try writeFileAtomic(path, out.items);
}

fn writeRawJsonlToPath(path: []const u8, nodes: []const NodeRecord) !void {
    var out = std.ArrayList(u8){};
    defer out.deinit(std.heap.c_allocator);
    const writer = out.writer(std.heap.c_allocator);
    for (nodes) |node| {
        try writer.writeAll(node.raw_json);
        try writer.writeByte('\n');
    }
    try writeFileAtomic(path, out.items);
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

fn resolveExportSourcesFormat(raw: ?[]const u8) !OutputFormat {
    const format = raw orelse "text";
    if (std.ascii.eqlIgnoreCase(format, "text")) return .text;
    if (std.ascii.eqlIgnoreCase(format, "json")) return .json;
    if (std.ascii.eqlIgnoreCase(format, "shell")) return .shell;
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

fn findNodeIndexByRawJson(nodes: []const NodeRecord, raw_json: []const u8) ?usize {
    for (nodes, 0..) |node, idx| {
        if (std.mem.eql(u8, node.raw_json, raw_json)) return idx;
    }
    return null;
}

fn findUniqueNodeIndexByPrimary(nodes: []const NodeRecord, primary: []const u8) ?usize {
    var found: ?usize = null;
    for (nodes, 0..) |node, idx| {
        if (!std.mem.eql(u8, node.identity_primary, primary)) continue;
        if (found != null) return null;
        found = idx;
    }
    return found;
}

fn findUniqueNodeIndexByScopeSecondary(nodes: []const NodeRecord, source_scope: []const u8, secondary: []const u8) ?usize {
    var found: ?usize = null;
    for (nodes, 0..) |node, idx| {
        if (!std.mem.eql(u8, node.source_scope, source_scope)) continue;
        if (!std.mem.eql(u8, node.identity_secondary, secondary)) continue;
        if (found != null) return null;
        found = idx;
    }
    return found;
}

fn resolveReusableNodeIndex(nodes: []const NodeRecord, hints: ImportHints) ?usize {
    if (hints.preferred_id) |value| {
        if (findNodeIndexById(nodes, value)) |idx| return idx;
    }
    if (hints.identity) |value| {
        if (findNodeIndexByIdentity(nodes, value)) |idx| return idx;
    }
    if (hints.source_scope) |scope| {
        if (hints.identity_secondary) |secondary| {
            if (findUniqueNodeIndexByScopeSecondary(nodes, scope, secondary)) |idx| return idx;
        }
    }
    if (hints.identity_primary) |primary| {
        if (findUniqueNodeIndexByPrimary(nodes, primary)) |idx| return idx;
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

    const old_order_csv = try joinNodeOrderCsvAlloc(allocator, old_nodes);
    defer allocator.free(old_order_csv);
    try reconcileReferenceState(allocator, state);
    const new_order_csv = try joinNodeOrderCsvAlloc(allocator, state.nodes);
    defer allocator.free(new_order_csv);

    var node_data_changed = false;

    for (old_nodes) |old_node| {
        if (findNodeIndexById(state.nodes, old_node.id) == null) {
            const key = try std.fmt.allocPrint(allocator, "fss_node_{s}", .{old_node.id});
            defer allocator.free(key);
            try client.removeKey(allocator, key);
            node_data_changed = true;
        }
    }

    for (state.nodes) |node| {
        if (findNodeIndexById(old_nodes, node.id)) |idx| {
            if (std.mem.eql(u8, old_nodes[idx].raw_json, node.raw_json)) continue;
        }
        const key = try std.fmt.allocPrint(allocator, "fss_node_{s}", .{node.id});
        defer allocator.free(key);
        const encoded = try encodeBase64StandardAlloc(allocator, node.raw_json);
        defer allocator.free(encoded);
        try client.setValue(allocator, key, encoded);
        node_data_changed = true;
    }

    const order_changed = !std.mem.eql(u8, old_order_csv, new_order_csv);
    if (order_changed) {
        if (new_order_csv.len > 0) {
            try client.setValue(allocator, "fss_node_order", new_order_csv);
        } else {
            try client.removeKey(allocator, "fss_node_order");
        }
    }

    const old_next_id = computeNextId(old_nodes);
    const new_next_id = computeNextId(state.nodes);
    const next_id_str = try std.fmt.allocPrint(allocator, "{d}", .{new_next_id});
    defer allocator.free(next_id_str);
    if (new_next_id != old_next_id) {
        try client.setValue(allocator, "fss_node_next_id", next_id_str);
    } else if (old_nodes.len == 0 and state.nodes.len > 0) {
        try client.setValue(allocator, "fss_node_next_id", next_id_str);
    }
    state.next_id = new_next_id;
    try client.setValue(allocator, "fss_data_schema", "2");

    if (node_data_changed or order_changed) {
        const ts = nowMs();
        const ts_str = try std.fmt.allocPrint(allocator, "{d}", .{ts});
        defer allocator.free(ts_str);
        try client.setValue(allocator, "fss_node_catalog_ts", ts_str);
        try client.setValue(allocator, "fss_node_config_ts", ts_str);
    }

    const current_before_id = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_current"));
    defer allocator.free(current_before_id);
    const current_before_identity = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_current_identity"));
    defer allocator.free(current_before_identity);
    if (!std.mem.eql(u8, current_before_id, state.current_id) or !std.mem.eql(u8, current_before_identity, state.current_identity)) {
        if (state.current_id.len > 0) {
            try client.setValue(allocator, "fss_node_current", state.current_id);
            try client.setValue(allocator, "fss_node_current_identity", state.current_identity);
        } else {
            try client.removeKey(allocator, "fss_node_current");
            try client.removeKey(allocator, "fss_node_current_identity");
        }
    }

    const failover_before_id = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_failover_backup"));
    defer allocator.free(failover_before_id);
    const failover_before_identity = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_failover_identity"));
    defer allocator.free(failover_before_identity);
    if (!std.mem.eql(u8, failover_before_id, state.failover_id) or !std.mem.eql(u8, failover_before_identity, state.failover_identity)) {
        if (state.failover_id.len > 0) {
            try client.setValue(allocator, "fss_node_failover_backup", state.failover_id);
            try client.setValue(allocator, "fss_node_failover_identity", state.failover_identity);
        } else {
            try client.removeKey(allocator, "fss_node_failover_backup");
            try client.removeKey(allocator, "fss_node_failover_identity");
        }
    }
}

fn reconcileReferenceState(allocator: std.mem.Allocator, state: *SchemaState) !void {
    var current_idx: ?usize = null;
    if (state.current_identity.len > 0) {
        current_idx = findNodeIndexByIdentity(state.nodes, state.current_identity);
    }
    if (current_idx == null and state.current_id.len > 0) {
        current_idx = findNodeIndexById(state.nodes, state.current_id);
    }

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

    var failover_idx: ?usize = null;
    if (state.failover_identity.len > 0) {
        failover_idx = findNodeIndexByIdentity(state.nodes, state.failover_identity);
    }
    if (failover_idx == null and state.failover_id.len > 0) {
        failover_idx = findNodeIndexById(state.nodes, state.failover_id);
    }

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

fn cloneSchemaState(allocator: std.mem.Allocator, state: SchemaState) !SchemaState {
    return .{
        .nodes = try cloneNodeSlice(allocator, state.nodes),
        .current_id = try allocator.dupe(u8, state.current_id),
        .current_identity = try allocator.dupe(u8, state.current_identity),
        .failover_id = try allocator.dupe(u8, state.failover_id),
        .failover_identity = try allocator.dupe(u8, state.failover_identity),
        .next_id = state.next_id,
    };
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

fn importNodeDocument(allocator: std.mem.Allocator, state: *SchemaState, raw_doc: []const u8, query: QueryOptions, position: []const u8, reuse_pool: ?[]const NodeRecord) !ImportResult {
    var hints = try extractImportHints(allocator, raw_doc);
    defer hints.deinit(allocator);

    var existing_idx: ?usize = null;
    var assigned_id: []u8 = undefined;
    const match_nodes = reuse_pool orelse state.nodes;

    if (query.reuse_ids) {
        existing_idx = resolveReusableNodeIndex(match_nodes, hints);
    }

    if (existing_idx) |idx| {
        assigned_id = try allocator.dupe(u8, match_nodes[idx].id);
    } else if (query.reuse_ids and hints.preferred_id != null and hints.preferred_id.?.len > 0 and findNodeIndexById(match_nodes, hints.preferred_id.?) == null) {
        assigned_id = try allocator.dupe(u8, hints.preferred_id.?);
    } else if (!query.reuse_ids and hints.preferred_id != null and hints.preferred_id.?.len > 0) {
        if (findNodeIndexById(state.nodes, hints.preferred_id.? ) != null) return error.NodeIdConflict;
        assigned_id = try allocator.dupe(u8, hints.preferred_id.?);
    } else {
        assigned_id = try std.fmt.allocPrint(allocator, "{d}", .{state.next_id});
        state.next_id += 1;
    }
    defer allocator.free(assigned_id);

    const existing_state_idx = findNodeIndexById(state.nodes, assigned_id);
    const existing_node = if (existing_state_idx) |idx| &state.nodes[idx] else if (existing_idx) |idx| &match_nodes[idx] else null;
    const normalized = try normalizeNodeJsonForWriteAlloc(allocator, raw_doc, assigned_id, query.source, existing_node);
    defer allocator.free(normalized);
    var new_record = try parseNodeRecordFromRawJson(allocator, normalized);

    if (existing_state_idx) |idx| {
        if (std.mem.eql(u8, state.nodes[idx].raw_json, new_record.raw_json)) {
            new_record.deinit(allocator);
            return .skipped;
        }
    } else {
        if (findNodeIndexByIdentity(state.nodes, new_record.identity)) |identity_idx| {
            if (query.reuse_ids and !std.mem.eql(u8, state.nodes[identity_idx].raw_json, new_record.raw_json)) {
                state.nodes[identity_idx].deinit(allocator);
                state.nodes[identity_idx] = new_record;
                return .updated;
            }
            new_record.deinit(allocator);
            return .skipped;
        }
        if (findNodeIndexByRawJson(state.nodes, new_record.raw_json)) |_| {
            new_record.deinit(allocator);
            return .skipped;
        }
    }

    if (existing_state_idx) |idx| {
        state.nodes[idx].deinit(allocator);
        state.nodes[idx] = new_record;
        return .updated;
    }

    try insertNodeAtPosition(allocator, &state.nodes, new_record, position);
    return .added;
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

fn printMutationSummary(command_name: []const u8, added: usize, updated: usize, removed: usize, skipped: usize, final_count: usize, dry_run: bool) !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("command: {s}\n", .{command_name});
    try stdout.print("dry_run: {s}\n", .{if (dry_run) "1" else "0"});
    try stdout.print("added: {d}\n", .{added});
    try stdout.print("updated: {d}\n", .{updated});
    try stdout.print("removed: {d}\n", .{removed});
    try stdout.print("skipped: {d}\n", .{skipped});
    try stdout.print("final_count: {d}\n", .{final_count});
}

fn reorderByIds(allocator: std.mem.Allocator, nodes: *[]NodeRecord, ids_csv: []const u8) !void {
    var ordered = std.ArrayList(NodeRecord){};
    defer ordered.deinit(allocator);
    var used = try allocator.alloc(bool, nodes.*.len);
    defer allocator.free(used);
    @memset(used, false);

    var matched: usize = 0;
    var it = std.mem.splitScalar(u8, ids_csv, ',');
    while (it.next()) |raw| {
        const id = std.mem.trim(u8, raw, " \t\r\n");
        if (id.len == 0) continue;
        if (findNodeIndexById(nodes.*, id)) |idx| {
            if (used[idx]) continue;
            try ordered.append(allocator, nodes.*[idx]);
            used[idx] = true;
            matched += 1;
        }
    }
    if (matched == 0) return error.InvalidArguments;
    for (nodes.*, 0..) |node, idx| {
        if (used[idx]) continue;
        try ordered.append(allocator, node);
    }
    allocator.free(nodes.*);
    nodes.* = try ordered.toOwnedSlice(allocator);
}

fn reorderBySort(allocator: std.mem.Allocator, nodes: *[]NodeRecord, query: QueryOptions, sort_key: []const u8) !void {
    const has_filter = query.source != null or query.source_tag != null or query.airport_identity != null or query.group != null or query.protocol != null or query.name != null or query.identity != null;
    if (!has_filter) {
        std.mem.sort(NodeRecord, nodes.*, SortContext{ .key = sort_key }, lessThanNodeRecord);
        return;
    }

    var positions = std.ArrayList(usize){};
    defer positions.deinit(allocator);
    var selected = std.ArrayList(NodeRecord){};
    defer selected.deinit(allocator);

    for (nodes.*, 0..) |node, idx| {
        if (!matchSource(query.source orelse "", node.source)) continue;
        if (!matchOptionalExact(query.source_tag orelse "", node.source_tag)) continue;
        if (!matchOptionalExact(query.airport_identity orelse "", node.airport_identity)) continue;
        if (!matchOptionalExact(query.group orelse "", node.group)) continue;
        if (!matchOptionalExactCaseFold(query.protocol orelse "", node.protocol)) continue;
        if (!matchOptionalExact(query.identity orelse "", node.identity)) continue;
        if (!matchOptionalSubstringCaseFold(query.name orelse "", node.name)) continue;
        try positions.append(allocator, idx);
        try selected.append(allocator, node);
    }
    if (selected.items.len == 0) return error.InvalidArguments;

    std.mem.sort(NodeRecord, selected.items, SortContext{ .key = sort_key }, lessThanNodeRecord);
    const out = try allocator.alloc(NodeRecord, nodes.*.len);
    for (nodes.*, 0..) |node, idx| out[idx] = node;
    for (positions.items, 0..) |pos, idx| out[pos] = selected.items[idx];
    allocator.free(nodes.*);
    nodes.* = out;
}

const SortContext = struct {
    key: []const u8,
};

fn lessThanNodeRecord(ctx: SortContext, lhs: NodeRecord, rhs: NodeRecord) bool {
    if (std.ascii.eqlIgnoreCase(ctx.key, "created")) return lhs.created_at < rhs.created_at;
    if (std.ascii.eqlIgnoreCase(ctx.key, "updated")) return lhs.updated_at < rhs.updated_at;
    if (std.ascii.eqlIgnoreCase(ctx.key, "id")) return std.fmt.parseInt(usize, lhs.id, 10) catch 0 < (std.fmt.parseInt(usize, rhs.id, 10) catch 0);
    if (std.ascii.eqlIgnoreCase(ctx.key, "protocol")) return std.mem.order(u8, lhs.protocol, rhs.protocol) == .lt;
    return std.ascii.orderIgnoreCase(lhs.name, rhs.name) == .lt;
}

fn buildPlanSummary(allocator: std.mem.Allocator, old_state: *const SchemaState, old_nodes: []const NodeRecord, new_state: *const SchemaState, new_nodes: []const NodeRecord) !PlanSummary {
    var added = std.ArrayList(PlanItem){};
    errdefer {
        for (added.items) |*item| item.deinit(allocator);
        added.deinit(allocator);
    }
    var updated = std.ArrayList(PlanItem){};
    errdefer {
        for (updated.items) |*item| item.deinit(allocator);
        updated.deinit(allocator);
    }
    var removed = std.ArrayList(PlanItem){};
    errdefer {
        for (removed.items) |*item| item.deinit(allocator);
        removed.deinit(allocator);
    }
    var moved = std.ArrayList(PlanItem){};
    errdefer {
        for (moved.items) |*item| item.deinit(allocator);
        moved.deinit(allocator);
    }
    var mappings = std.ArrayList(ReconcileMapItem){};
    errdefer {
        for (mappings.items) |*item| item.deinit(allocator);
        mappings.deinit(allocator);
    }
    var unmapped = std.ArrayList(ReconcileMapItem){};
    errdefer {
        for (unmapped.items) |*item| item.deinit(allocator);
        unmapped.deinit(allocator);
    }

    for (new_nodes) |node| {
        if (findNodeIndexById(old_nodes, node.id)) |idx| {
            if (!std.mem.eql(u8, old_nodes[idx].raw_json, node.raw_json)) {
                try updated.append(allocator, try makePlanItemWithChanges(allocator, old_nodes[idx], node));
            }
        } else {
            try added.append(allocator, try makePlanItem(allocator, node));
        }
    }

    for (old_nodes) |node| {
        if (findNodeIndexById(new_nodes, node.id) == null) {
            try removed.append(allocator, try makePlanItem(allocator, node));
        }
    }

    for (new_nodes, 0..) |node, new_idx| {
        if (findNodeIndexById(old_nodes, node.id)) |old_idx| {
            if (old_idx != new_idx) {
                try moved.append(allocator, try makePlanItem(allocator, node));
            }
        }
    }

    const final_order = try joinNodeOrderCsvAlloc(allocator, new_nodes);
    const old_order = try joinNodeOrderCsvAlloc(allocator, old_nodes);
    defer allocator.free(old_order);
    try buildReferenceMappings(allocator, old_nodes, new_nodes, &mappings, &unmapped);

    return .{
        .added = added,
        .updated = updated,
        .removed = removed,
        .moved = moved,
        .mappings = mappings,
        .unmapped = unmapped,
        .current_before_id = try allocator.dupe(u8, old_state.current_id),
        .current_before_identity = try allocator.dupe(u8, old_state.current_identity),
        .current_after_id = try allocator.dupe(u8, new_state.current_id),
        .current_after_identity = try allocator.dupe(u8, new_state.current_identity),
        .failover_before_id = try allocator.dupe(u8, old_state.failover_id),
        .failover_before_identity = try allocator.dupe(u8, old_state.failover_identity),
        .failover_after_id = try allocator.dupe(u8, new_state.failover_id),
        .failover_after_identity = try allocator.dupe(u8, new_state.failover_identity),
        .final_order = final_order,
        .final_count = new_nodes.len,
        .order_changed = !std.mem.eql(u8, old_order, final_order),
    };
}

fn makePlanItem(allocator: std.mem.Allocator, node: NodeRecord) !PlanItem {
    return .{
        .id = try allocator.dupe(u8, node.id),
        .name = try allocator.dupe(u8, node.name),
        .protocol = try allocator.dupe(u8, node.protocol),
        .identity = try allocator.dupe(u8, node.identity),
        .changes = std.ArrayList(PlanFieldChange){},
    };
}

fn makePlanItemWithChanges(allocator: std.mem.Allocator, old_node: NodeRecord, new_node: NodeRecord) !PlanItem {
    var item = try makePlanItem(allocator, new_node);
    errdefer item.deinit(allocator);

    for (tracked_plan_fields) |field| {
        const old_value = try extractStringFieldAlloc(allocator, old_node.raw_json, field);
        defer if (old_value) |value| allocator.free(value);
        const new_value = try extractStringFieldAlloc(allocator, new_node.raw_json, field);
        defer if (new_value) |value| allocator.free(value);

        const lhs = if (old_value) |value| value else "";
        const rhs = if (new_value) |value| value else "";
        if (std.mem.eql(u8, lhs, rhs)) continue;
        try item.changes.append(allocator, .{
            .field = try allocator.dupe(u8, field),
            .old_value = try allocator.dupe(u8, lhs),
            .new_value = try allocator.dupe(u8, rhs),
        });
    }
    return item;
}

fn makeReconcileMapItem(allocator: std.mem.Allocator, reason: []const u8, old_node: NodeRecord, new_node: ?NodeRecord) !ReconcileMapItem {
    return .{
        .match_reason = try allocator.dupe(u8, reason),
        .old_id = try allocator.dupe(u8, old_node.id),
        .old_identity = try allocator.dupe(u8, old_node.identity),
        .old_name = try allocator.dupe(u8, old_node.name),
        .new_id = try allocator.dupe(u8, if (new_node) |node| node.id else ""),
        .new_identity = try allocator.dupe(u8, if (new_node) |node| node.identity else ""),
        .new_name = try allocator.dupe(u8, if (new_node) |node| node.name else ""),
    };
}

fn findFirstUnusedByIdentity(nodes: []const NodeRecord, used: []bool, identity: []const u8) ?usize {
    if (identity.len == 0) return null;
    for (nodes, 0..) |node, idx| {
        if (used[idx]) continue;
        if (std.mem.eql(u8, node.identity, identity)) return idx;
    }
    return null;
}

fn findUniqueUnusedByPrimary(nodes: []const NodeRecord, used: []bool, primary: []const u8) ?usize {
    if (primary.len == 0) return null;
    var found: ?usize = null;
    for (nodes, 0..) |node, idx| {
        if (used[idx]) continue;
        if (!std.mem.eql(u8, node.identity_primary, primary)) continue;
        if (found != null) return null;
        found = idx;
    }
    return found;
}

fn findUniqueUnusedByScopeSecondary(nodes: []const NodeRecord, used: []bool, source_scope: []const u8, secondary: []const u8) ?usize {
    if (source_scope.len == 0 or secondary.len == 0) return null;
    var found: ?usize = null;
    for (nodes, 0..) |node, idx| {
        if (used[idx]) continue;
        if (!std.mem.eql(u8, node.source_scope, source_scope)) continue;
        if (!std.mem.eql(u8, node.identity_secondary, secondary)) continue;
        if (found != null) return null;
        found = idx;
    }
    return found;
}

fn buildReferenceMappings(allocator: std.mem.Allocator, old_nodes: []const NodeRecord, new_nodes: []const NodeRecord, mappings: *std.ArrayList(ReconcileMapItem), unmapped: *std.ArrayList(ReconcileMapItem)) !void {
    var old_used = try allocator.alloc(bool, old_nodes.len);
    defer allocator.free(old_used);
    @memset(old_used, false);
    var new_used = try allocator.alloc(bool, new_nodes.len);
    defer allocator.free(new_used);
    @memset(new_used, false);

    for (old_nodes, 0..) |old_node, old_idx| {
        if (findFirstUnusedByIdentity(new_nodes, new_used, old_node.identity)) |new_idx| {
            try mappings.append(allocator, try makeReconcileMapItem(allocator, "identity", old_node, new_nodes[new_idx]));
            old_used[old_idx] = true;
            new_used[new_idx] = true;
        }
    }

    for (old_nodes, 0..) |old_node, old_idx| {
        if (old_used[old_idx]) continue;
        if (findUniqueUnusedByPrimary(new_nodes, new_used, old_node.identity_primary)) |new_idx| {
            try mappings.append(allocator, try makeReconcileMapItem(allocator, "primary", old_node, new_nodes[new_idx]));
            old_used[old_idx] = true;
            new_used[new_idx] = true;
        }
    }

    for (old_nodes, 0..) |old_node, old_idx| {
        if (old_used[old_idx]) continue;
        if (findUniqueUnusedByScopeSecondary(new_nodes, new_used, old_node.source_scope, old_node.identity_secondary)) |new_idx| {
            try mappings.append(allocator, try makeReconcileMapItem(allocator, "scope_secondary", old_node, new_nodes[new_idx]));
            old_used[old_idx] = true;
            new_used[new_idx] = true;
        }
    }

    for (old_nodes, 0..) |old_node, old_idx| {
        if (old_used[old_idx]) continue;
        try unmapped.append(allocator, try makeReconcileMapItem(allocator, "missing", old_node, null));
    }
}

fn writePlanText(writer: anytype, plan: *PlanSummary) !void {
    try writer.print("added: {d}\n", .{plan.added.items.len});
    for (plan.added.items) |item| try writer.print("  + {s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.name });
    try writer.print("updated: {d}\n", .{plan.updated.items.len});
    for (plan.updated.items) |item| {
        try writer.print("  ~ {s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.name });
        for (item.changes.items) |change| {
            try writer.print("    · {s}: {s} -> {s}\n", .{ change.field, change.old_value, change.new_value });
        }
    }
    try writer.print("removed: {d}\n", .{plan.removed.items.len});
    for (plan.removed.items) |item| try writer.print("  - {s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.name });
    try writer.print("moved: {d}\n", .{plan.moved.items.len});
    for (plan.moved.items) |item| try writer.print("  > {s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.name });
    try writer.print("mappings: {d}\n", .{plan.mappings.items.len});
    for (plan.mappings.items) |item| try writer.print("  = {s}\t{s}\t{s}\t{s}\t{s}\n", .{ item.match_reason, item.old_id, item.old_name, item.new_id, item.new_name });
    try writer.print("unmapped: {d}\n", .{plan.unmapped.items.len});
    for (plan.unmapped.items) |item| try writer.print("  ! {s}\t{s}\t{s}\n", .{ item.old_id, item.old_identity, item.old_name });
    try writer.print("final_count: {d}\n", .{plan.final_count});
    try writer.print("order_changed: {s}\n", .{if (plan.order_changed) "1" else "0"});
    try writer.print("current_before: {s} {s}\n", .{ plan.current_before_id, plan.current_before_identity });
    try writer.print("current_after: {s} {s}\n", .{ plan.current_after_id, plan.current_after_identity });
    try writer.print("failover_before: {s} {s}\n", .{ plan.failover_before_id, plan.failover_before_identity });
    try writer.print("failover_after: {s} {s}\n", .{ plan.failover_after_id, plan.failover_after_identity });
    try writer.print("final_order: {s}\n", .{plan.final_order});
}

fn writePlanJson(allocator: std.mem.Allocator, writer: anytype, plan: *PlanSummary) !void {
    try writer.writeAll("{\"added\":[");
    for (plan.added.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        const rendered = try renderPlanItemJsonAlloc(allocator, item);
        defer allocator.free(rendered);
        try writer.writeAll(rendered);
    }
    try writer.writeAll("],\"updated\":[");
    for (plan.updated.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        const rendered = try renderPlanItemJsonAlloc(allocator, item);
        defer allocator.free(rendered);
        try writer.writeAll(rendered);
    }
    try writer.writeAll("],\"removed\":[");
    for (plan.removed.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        const rendered = try renderPlanItemJsonAlloc(allocator, item);
        defer allocator.free(rendered);
        try writer.writeAll(rendered);
    }
    try writer.writeAll("],\"moved\":[");
    for (plan.moved.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        const rendered = try renderPlanItemJsonAlloc(allocator, item);
        defer allocator.free(rendered);
        try writer.writeAll(rendered);
    }
    try writer.writeAll("],\"mappings\":[");
    for (plan.mappings.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writeReconcileMapItemJson(writer, item);
    }
    try writer.writeAll("],\"unmapped\":[");
    for (plan.unmapped.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writeReconcileMapItemJson(writer, item);
    }
    try writer.print("],\"final_count\":{d},\"order_changed\":{s},\"current_before\":{{\"id\":", .{
        plan.final_count,
        if (plan.order_changed) "true" else "false",
    });
    try writeJsonString(writer, plan.current_before_id);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, plan.current_before_identity);
    try writer.writeAll("},\"current_after\":{\"id\":");
    try writeJsonString(writer, plan.current_after_id);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, plan.current_after_identity);
    try writer.writeAll("},\"failover_before\":{\"id\":");
    try writeJsonString(writer, plan.failover_before_id);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, plan.failover_before_identity);
    try writer.writeAll("},\"failover_after\":{\"id\":");
    try writeJsonString(writer, plan.failover_after_id);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, plan.failover_after_identity);
    try writer.writeAll("},\"final_order\":");
    try writeJsonString(writer, plan.final_order);
    try writer.writeAll("}\n");
}

fn writePlanShell(writer: anytype, plan: *PlanSummary) !void {
    try writer.print("summary\t{d}\t{d}\t{d}\t{d}\t{d}\t{s}\n", .{
        plan.added.items.len,
        plan.updated.items.len,
        plan.removed.items.len,
        plan.moved.items.len,
        plan.final_count,
        if (plan.order_changed) "1" else "0",
    });
    for (plan.added.items) |item| {
        try writer.print("add\t{s}\t{s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.identity, item.name });
    }
    for (plan.updated.items) |item| {
        try writer.print("update\t{s}\t{s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.identity, item.name });
        for (item.changes.items) |change| {
            try writer.print("field\t{s}\t{s}\t{s}\t{s}\n", .{ item.id, change.field, change.old_value, change.new_value });
        }
    }
    for (plan.removed.items) |item| {
        try writer.print("remove\t{s}\t{s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.identity, item.name });
    }
    for (plan.moved.items) |item| {
        try writer.print("move\t{s}\t{s}\t{s}\t{s}\n", .{ item.id, item.protocol, item.identity, item.name });
    }
    for (plan.mappings.items) |item| {
        try writer.print("map\t{s}\t{s}\t{s}\t{s}\t{s}\t{s}\t{s}\n", .{
            item.match_reason,
            item.old_id,
            item.old_identity,
            item.new_id,
            item.new_identity,
            item.old_name,
            item.new_name,
        });
    }
    for (plan.unmapped.items) |item| {
        try writer.print("unmapped\t{s}\t{s}\t{s}\n", .{
            item.old_id,
            item.old_identity,
            item.old_name,
        });
    }
    try writer.print("current\t{s}\t{s}\t{s}\t{s}\n", .{
        plan.current_before_id,
        plan.current_before_identity,
        plan.current_after_id,
        plan.current_after_identity,
    });
    try writer.print("failover\t{s}\t{s}\t{s}\t{s}\n", .{
        plan.failover_before_id,
        plan.failover_before_identity,
        plan.failover_after_id,
        plan.failover_after_identity,
    });
    try writer.print("final_order\t{s}\n", .{plan.final_order});
}

fn writeReconcileMapItemJson(writer: anytype, item: ReconcileMapItem) !void {
    try writer.writeAll("{\"reason\":");
    try writeJsonString(writer, item.match_reason);
    try writer.writeAll(",\"old_id\":");
    try writeJsonString(writer, item.old_id);
    try writer.writeAll(",\"old_identity\":");
    try writeJsonString(writer, item.old_identity);
    try writer.writeAll(",\"old_name\":");
    try writeJsonString(writer, item.old_name);
    try writer.writeAll(",\"new_id\":");
    try writeJsonString(writer, item.new_id);
    try writer.writeAll(",\"new_identity\":");
    try writeJsonString(writer, item.new_identity);
    try writer.writeAll(",\"new_name\":");
    try writeJsonString(writer, item.new_name);
    try writer.writeAll("}");
}

fn renderPlanItemJsonAlloc(allocator: std.mem.Allocator, item: PlanItem) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    try writer.writeAll("{\"id\":");
    try writeJsonString(writer, item.id);
    try writer.writeAll(",\"protocol\":");
    try writeJsonString(writer, item.protocol);
    try writer.writeAll(",\"name\":");
    try writeJsonString(writer, item.name);
    try writer.writeAll(",\"identity\":");
    try writeJsonString(writer, item.identity);
    try writer.writeAll(",\"changes\":[");
    for (item.changes.items, 0..) |change, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.writeAll("{\"field\":");
        try writeJsonString(writer, change.field);
        try writer.writeAll(",\"old\":");
        try writeJsonString(writer, change.old_value);
        try writer.writeAll(",\"new\":");
        try writeJsonString(writer, change.new_value);
        try writer.writeAll("}");
    }
    try writer.writeAll("]");
    try writer.writeAll("}");
    return try out.toOwnedSlice(allocator);
}

fn findDuplicates(allocator: std.mem.Allocator, nodes: []const NodeRecord, match_raw: ?[]const u8) !DuplicateSummary {
    var items = std.ArrayList(DuplicateItem){};
    errdefer {
        for (items.items) |*item| item.deinit(allocator);
        items.deinit(allocator);
    }

    const do_identity = match_raw == null or std.ascii.eqlIgnoreCase(match_raw.?, "identity") or std.ascii.eqlIgnoreCase(match_raw.?, "all");
    const do_config = match_raw == null or std.ascii.eqlIgnoreCase(match_raw.?, "config") or std.ascii.eqlIgnoreCase(match_raw.?, "all");
    if (!do_identity and !do_config) return error.InvalidArguments;

    var removed = try allocator.alloc(bool, nodes.len);
    defer allocator.free(removed);
    @memset(removed, false);

    if (do_identity) {
        var keep_idx: usize = 0;
        while (keep_idx < nodes.len) : (keep_idx += 1) {
            const keep_node = nodes[keep_idx];
            if (removed[keep_idx]) continue;
            if (keep_node.identity.len == 0) continue;
            var remove_idx = keep_idx + 1;
            while (remove_idx < nodes.len) : (remove_idx += 1) {
                const remove_node = nodes[remove_idx];
                if (removed[remove_idx]) continue;
                if (!std.mem.eql(u8, keep_node.identity, remove_node.identity)) continue;
                try items.append(allocator, try makeDuplicateItem(allocator, .identity, keep_node, remove_node, keep_node.identity));
                removed[remove_idx] = true;
            }
        }
    }

    if (do_config) {
        var keep_idx: usize = 0;
        while (keep_idx < nodes.len) : (keep_idx += 1) {
            const keep_node = nodes[keep_idx];
            if (removed[keep_idx]) continue;
            if (keep_node.identity_secondary.len == 0) continue;
            var remove_idx = keep_idx + 1;
            while (remove_idx < nodes.len) : (remove_idx += 1) {
                const remove_node = nodes[remove_idx];
                if (removed[remove_idx]) continue;
                if (!std.mem.eql(u8, keep_node.identity_secondary, remove_node.identity_secondary)) continue;
                try items.append(allocator, try makeDuplicateItem(allocator, .config, keep_node, remove_node, keep_node.identity_secondary));
                removed[remove_idx] = true;
            }
        }
    }

    return .{ .items = items };
}

fn makeDuplicateItem(allocator: std.mem.Allocator, kind: DuplicateKind, keep_node: NodeRecord, remove_node: NodeRecord, signature: []const u8) !DuplicateItem {
    return .{
        .kind = kind,
        .keep_id = try allocator.dupe(u8, keep_node.id),
        .keep_name = try allocator.dupe(u8, keep_node.name),
        .remove_id = try allocator.dupe(u8, remove_node.id),
        .remove_name = try allocator.dupe(u8, remove_node.name),
        .signature = try allocator.dupe(u8, signature),
    };
}

fn duplicateKindLabel(kind: DuplicateKind) []const u8 {
    return switch (kind) {
        .identity => "identity",
        .config => "config",
    };
}

fn writeDuplicateText(writer: anytype, dupes: *DuplicateSummary) !void {
    try writer.print("duplicates: {d}\n", .{dupes.items.items.len});
    for (dupes.items.items) |item| {
        try writer.print("- {s}\tkeep={s}:{s}\tremove={s}:{s}\tsig={s}\n", .{
            duplicateKindLabel(item.kind),
            item.keep_id,
            item.keep_name,
            item.remove_id,
            item.remove_name,
            item.signature,
        });
    }
}

fn writeDuplicateShell(writer: anytype, dupes: *DuplicateSummary) !void {
    try writer.print("summary\t{d}\n", .{dupes.items.items.len});
    for (dupes.items.items) |item| {
        try writer.print("duplicate\t{s}\t{s}\t{s}\t{s}\t{s}\t{s}\n", .{
            duplicateKindLabel(item.kind),
            item.keep_id,
            item.keep_name,
            item.remove_id,
            item.remove_name,
            item.signature,
        });
    }
}

fn writeDuplicateJson(allocator: std.mem.Allocator, writer: anytype, dupes: *DuplicateSummary) !void {
    try writer.writeAll("{\"duplicates\":[");
    for (dupes.items.items, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        var out = std.ArrayList(u8){};
        defer out.deinit(allocator);
        const w = out.writer(allocator);
        try w.writeAll("{\"kind\":");
        try writeJsonString(w, duplicateKindLabel(item.kind));
        try w.writeAll(",\"keep_id\":");
        try writeJsonString(w, item.keep_id);
        try w.writeAll(",\"keep_name\":");
        try writeJsonString(w, item.keep_name);
        try w.writeAll(",\"remove_id\":");
        try writeJsonString(w, item.remove_id);
        try w.writeAll(",\"remove_name\":");
        try writeJsonString(w, item.remove_name);
        try w.writeAll(",\"signature\":");
        try writeJsonString(w, item.signature);
        try w.writeAll("}");
        try writer.writeAll(out.items);
    }
    try writer.writeAll("]}\n");
}

fn deleteByExactId(allocator: std.mem.Allocator, state: *SchemaState, id: []const u8) !usize {
    if (findNodeIndexById(state.nodes, id)) |idx| {
        var kept = std.ArrayList(NodeRecord){};
        errdefer kept.deinit(allocator);
        for (state.nodes, 0..) |*node, cur_idx| {
            if (cur_idx == idx) {
                node.deinit(allocator);
            } else {
                try kept.append(allocator, node.*);
            }
        }
        allocator.free(state.nodes);
        state.nodes = try kept.toOwnedSlice(allocator);
        return 1;
    }
    return 0;
}

fn warmJsonCache(allocator: std.mem.Allocator, nodes: []const NodeRecord) !usize {
    const cache_dir = "/koolshare/configs/fancyss/node_json_cache";
    const tmp_dir = "/koolshare/configs/fancyss/node_json_cache.tmp.node_tool";
    const old_dir = "/koolshare/configs/fancyss/node_json_cache.old.node_tool";
    const meta_file = "/koolshare/configs/fancyss/node_json_cache.meta";
    const index_file = "/koolshare/configs/fancyss/node_json_cache/nodes_index.txt";
    _ = index_file;

    try ensureCleanDir(tmp_dir);
    for (nodes) |node| {
        const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}.json", .{ tmp_dir, node.id });
        defer allocator.free(file_path);
        try writeFileAtomic(file_path, node.raw_json);
    }
    try writeJsonIndexFile(allocator, tmp_dir, nodes);
    try replaceDirAtomic(cache_dir, tmp_dir, old_dir);
    try writeSimpleMeta(meta_file, "config_ts", nowMs());
    return nodes.len;
}

fn warmEnvCache(allocator: std.mem.Allocator, nodes: []const NodeRecord) !usize {
    const cache_dir = "/koolshare/configs/fancyss/node_env_cache";
    const tmp_dir = "/koolshare/configs/fancyss/node_env_cache.tmp.node_tool";
    const old_dir = "/koolshare/configs/fancyss/node_env_cache.old.node_tool";
    const meta_file = "/koolshare/configs/fancyss/node_env_cache.meta";
    const obfs_file = "/koolshare/configs/fancyss/node_env_cache/ss_obfs_ids.txt";

    try ensureCleanDir(tmp_dir);
    var obfs_ids = std.ArrayList(u8){};
    defer obfs_ids.deinit(allocator);

    for (nodes) |node| {
        const env_path = try std.fmt.allocPrint(allocator, "{s}/{s}.env", .{ tmp_dir, node.id });
        defer allocator.free(env_path);
        const env_content = try renderNodeEnvAlloc(allocator, node.raw_json);
        defer allocator.free(env_content);
        try writeFileAtomic(env_path, env_content);

        if (std.mem.eql(u8, node.type_id, "0")) {
            if (containsSsObfs(node.raw_json)) {
                if (obfs_ids.items.len > 0) try obfs_ids.append(allocator, '\n');
                try obfs_ids.appendSlice(allocator, node.id);
            }
        }
    }

    const obfs_path = try allocator.dupe(u8, try std.fmt.allocPrint(allocator, "{s}/ss_obfs_ids.txt", .{tmp_dir}));
    defer allocator.free(obfs_path);
    try writeFileAtomic(obfs_path, obfs_ids.items);
    try replaceDirAtomic(cache_dir, tmp_dir, old_dir);
    try writeSimpleMeta(meta_file, "config_ts", nowMs());
    _ = obfs_file;
    return nodes.len;
}

fn warmDirectDomainsCache(allocator: std.mem.Allocator, nodes: []const NodeRecord) !usize {
    const cache_file = "/koolshare/configs/fancyss/node_direct_domains.txt";
    const meta_file = "/koolshare/configs/fancyss/node_direct_domains.meta";
    var domains = std.ArrayList([]const u8){};
    defer domains.deinit(allocator);

    for (nodes) |node| {
        if (node.server.len == 0) continue;
        if (!looksLikeDomain(node.server)) continue;
        if (containsSlice(domains.items, node.server)) continue;
        try domains.append(allocator, node.server);
    }

    std.mem.sort([]const u8, domains.items, {}, lessThanString);
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    for (domains.items, 0..) |domain, idx| {
        if (idx > 0) try writer.writeByte('\n');
        try writer.writeAll(domain);
    }
    if (out.items.len > 0) try writer.writeByte('\n');
    try writeFileAtomic(cache_file, out.items);
    try writeSimpleMeta(meta_file, "catalog_ts", nowMs());
    return domains.items.len;
}

fn ensureCleanDir(path: []const u8) !void {
    std.fs.cwd().deleteTree(path) catch {};
    try ensureDirExistsCompat(path);
}

fn ensureDirExistsCompat(path: []const u8) !void {
    std.fs.cwd().makePath(path) catch |err| {
        if (std.fs.cwd().openDir(path, .{})) |opened_dir| {
            var dir = opened_dir;
            dir.close();
        } else |_| {
            return err;
        }
    };
}

fn replaceDirAtomic(target: []const u8, tmp_dir: []const u8, old_dir: []const u8) !void {
    std.fs.cwd().deleteTree(old_dir) catch {};
    std.fs.cwd().rename(target, old_dir) catch {};
    try std.fs.cwd().rename(tmp_dir, target);
    std.fs.cwd().deleteTree(old_dir) catch {};
}

fn writeFileAtomic(path: []const u8, content: []const u8) !void {
    var file = try std.fs.cwd().createFile(path, .{ .truncate = true });
    defer file.close();
    try file.writeAll(content);
}

fn writeSimpleMeta(path: []const u8, key: []const u8, value: u64) !void {
    const content = try std.fmt.allocPrint(std.heap.c_allocator, "{s}={d}\n", .{ key, value });
    defer std.heap.c_allocator.free(content);
    try writeFileAtomic(path, content);
}

fn writeJsonIndexFile(allocator: std.mem.Allocator, dir_path: []const u8, nodes: []const NodeRecord) !void {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    for (nodes) |node| {
        const method = try extractStringFieldAlloc(allocator, node.raw_json, "method");
        defer if (method) |value| allocator.free(value);
        const ss_obfs = try extractStringFieldAlloc(allocator, node.raw_json, "ss_obfs");
        defer if (ss_obfs) |value| allocator.free(value);
        const type_padded = if (node.type_id.len == 1)
            try std.fmt.allocPrint(allocator, "0{s}", .{node.type_id})
        else
            try allocator.dupe(u8, node.type_id);
        defer allocator.free(type_padded);
        try writer.print("{s}|{s}|{s}|{s}\n", .{
            node.id,
            type_padded,
            if (ss_obfs) |value| value else "",
            if (method) |value| value else "",
        });
    }
    const path = try std.fmt.allocPrint(allocator, "{s}/nodes_index.txt", .{dir_path});
    defer allocator.free(path);
    try writeFileAtomic(path, out.items);
}

fn renderNodeEnvAlloc(allocator: std.mem.Allocator, raw_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, raw_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidArguments;

    var keys = std.ArrayList([]const u8){};
    defer keys.deinit(allocator);
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len == 0) continue;
        if (entry.key_ptr.*[0] == '_') continue;
        if (entry.value_ptr.* == .null) continue;
        const rendered = try jsonValueToPlainStringAlloc(allocator, entry.value_ptr.*);
        defer allocator.free(rendered);
        if (rendered.len == 0) continue;
        try keys.append(allocator, try allocator.dupe(u8, entry.key_ptr.*));
    }
    defer for (keys.items) |key| allocator.free(key);
    std.mem.sort([]const u8, keys.items, {}, lessThanString);

    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    try writer.writeAll("WT_NODE_ENV_FIELDS=");
    for (keys.items, 0..) |key, idx| {
        if (idx > 0) try writer.writeByte(' ');
        try writer.writeAll(key);
    }
    try writer.writeByte('\n');

    for (keys.items) |key| {
        const value = parsed.value.object.get(key).?;
        const plain = try jsonValueToPlainStringAlloc(allocator, value);
        defer allocator.free(plain);
        const quoted = try shellQuoteAlloc(allocator, plain);
        defer allocator.free(quoted);
        try writer.print("WTN_{s}={s}\n", .{ key, quoted });
    }
    return try out.toOwnedSlice(allocator);
}

fn jsonValueToPlainStringAlloc(allocator: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return switch (value) {
        .string => try allocator.dupe(u8, value.string),
        .integer => try std.fmt.allocPrint(allocator, "{d}", .{value.integer}),
        .float => try std.fmt.allocPrint(allocator, "{d}", .{value.float}),
        .bool => try allocator.dupe(u8, if (value.bool) "1" else "0"),
        .number_string => try allocator.dupe(u8, value.number_string),
        .null => try allocator.dupe(u8, ""),
        else => try renderCompactJsonAlloc(allocator, value),
    };
}

fn shellQuoteAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    try writer.writeByte('\'');
    for (input) |ch| {
        if (ch == '\'') {
            try writer.writeAll("'\\''");
        } else {
            try writer.writeByte(ch);
        }
    }
    try writer.writeByte('\'');
    return try out.toOwnedSlice(allocator);
}

fn containsSsObfs(raw_json: []const u8) bool {
    var parsed = std.json.parseFromSlice(std.json.Value, std.heap.c_allocator, raw_json, .{}) catch return false;
    defer parsed.deinit();
    if (parsed.value != .object) return false;
    const value = parsed.value.object.get("ss_obfs") orelse return false;
    if (value != .string) return false;
    return std.mem.eql(u8, value.string, "http") or std.mem.eql(u8, value.string, "tls");
}

fn looksLikeDomain(host: []const u8) bool {
    if (host.len == 0) return false;
    if (std.mem.indexOfScalar(u8, host, ':') != null) return false;
    if (std.mem.indexOfScalar(u8, host, '.') == null) return false;
    var all_numeric_or_dot = true;
    for (host) |ch| {
        if ((ch >= '0' and ch <= '9') or ch == '.') continue;
        all_numeric_or_dot = false;
        break;
    }
    return !all_numeric_or_dot;
}

fn containsSlice(items: []const []const u8, value: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, value)) return true;
    }
    return false;
}

fn warmWebtestArtifacts(allocator: std.mem.Allocator, socket_path: []const u8, nodes: []const NodeRecord) !usize {
    const cache_dir = "/koolshare/configs/fancyss/webtest_cache";
    const node_dir = "/koolshare/configs/fancyss/webtest_cache/nodes";
    const meta_dir = "/koolshare/configs/fancyss/webtest_cache/meta";
    const index_file = "/koolshare/configs/fancyss/webtest_cache/materialize_index.txt";
    const agg_file = "/koolshare/configs/fancyss/webtest_cache/all_outbounds.json";
    const global_meta_file = "/koolshare/configs/fancyss/webtest_cache/cache.meta";

    try ensureDirExistsCompat(cache_dir);
    try ensureDirExistsCompat(node_dir);
    try ensureDirExistsCompat(meta_dir);

    var ids = std.ArrayList([]const u8){};
    defer ids.deinit(allocator);
    for (nodes) |node| {
        if (!isWebtestXrayLike(node)) continue;
        try ids.append(allocator, node.id);
    }
    std.mem.sort([]const u8, ids.items, {}, lessThanNumericString);

    if (ids.items.len == 0) {
        std.fs.cwd().deleteFile(index_file) catch {};
        std.fs.cwd().deleteFile(agg_file) catch {};
        std.fs.cwd().deleteFile(global_meta_file) catch {};
        return 0;
    }

    const ids_file = try std.fmt.allocPrint(allocator, "/tmp/node_tool_webtest_ids_{d}.txt", .{std.time.microTimestamp()});
    defer {
        std.fs.cwd().deleteFile(ids_file) catch {};
        allocator.free(ids_file);
    }
    const ids_blob = try joinIdsWithNewlineAlloc(allocator, ids.items);
    defer allocator.free(ids_blob);
    try writeFileAtomic(ids_file, ids_blob);
    const build_ids_file = try std.fmt.allocPrint(allocator, "/tmp/node_tool_webtest_build_ids_{d}.txt", .{std.time.microTimestamp()});
    defer {
        std.fs.cwd().deleteFile(build_ids_file) catch {};
        allocator.free(build_ids_file);
    }
    if (try webtestGlobalMetaMatches(allocator, socket_path, global_meta_file, ids.items)) {
        try collectMissingWebtestIds(allocator, node_dir, meta_dir, nodes, ids.items, build_ids_file);
    } else {
        try writeFileAtomic(build_ids_file, ids_blob);
    }

    const build_content = try readFileAllocCompat(allocator, build_ids_file, max_skipd_frame);
    defer allocator.free(build_content);
    if (std.mem.trim(u8, build_content, " \t\r\n").len > 0) {
        var runtime_cfg = try loadWebtestRuntimeConfig(allocator, socket_path);
        defer runtime_cfg.deinit(allocator);
        try buildWebtestNodeArtifactsWithShell(allocator, build_ids_file, nodes, node_dir, meta_dir, runtime_cfg);
    }

    var index = std.ArrayList(u8){};
    defer index.deinit(allocator);
    var aggregate = std.ArrayList(u8){};
    defer aggregate.deinit(allocator);
    const agg_writer = aggregate.writer(allocator);
    try agg_writer.writeAll("{\n  \"outbounds\": [\n");
    var first_out = true;

    for (ids.items) |node_id| {
        const meta_path = try std.fmt.allocPrint(allocator, "{s}/{s}.meta", .{ meta_dir, node_id });
        defer allocator.free(meta_path);
        const out_path = try std.fmt.allocPrint(allocator, "{s}/{s}_outbounds.json", .{ node_dir, node_id });
        defer allocator.free(out_path);
        if (!fileExists(meta_path) or !fileExists(out_path)) continue;

        const start_port = try readMetaValueAlloc(allocator, meta_path, "start_port");
        defer allocator.free(start_port);
        if (index.items.len > 0) try index.append(allocator, '\n');
        try index.appendSlice(allocator, node_id);
        try index.append(allocator, '|');
        try index.appendSlice(allocator, start_port);

        const out_json = try readFileAllocCompat(allocator, out_path, max_skipd_frame);
        defer allocator.free(out_json);
        if (!first_out) try agg_writer.writeAll(",\n");
        first_out = false;
        try agg_writer.writeAll(out_json);
    }
    try agg_writer.writeAll("\n  ]\n}\n");

    if (index.items.len > 0) {
        try writeFileAtomic(index_file, index.items);
        try writeFileAtomic(agg_file, aggregate.items);
    } else {
        std.fs.cwd().deleteFile(index_file) catch {};
        std.fs.cwd().deleteFile(agg_file) catch {};
    }

    try writeWebtestGlobalMeta(allocator, socket_path, global_meta_file, ids.items);
    return ids.items.len;
}

fn webtestGlobalMetaMatches(allocator: std.mem.Allocator, socket_path: []const u8, path: []const u8, ids: []const []const u8) !bool {
    if (!fileExists(path)) return false;

    const cache_rev = try readMetaValueAlloc(allocator, path, "cache_rev");
    defer allocator.free(cache_rev);
    const gen_rev = try readMetaValueAlloc(allocator, path, "gen_rev");
    defer allocator.free(gen_rev);
    const linux_ver = try readMetaValueAlloc(allocator, path, "linux_ver");
    defer allocator.free(linux_ver);
    const cache_tfo = try readMetaValueAlloc(allocator, path, "ss_basic_tfo");
    defer allocator.free(cache_tfo);
    const cache_resolv_mode = try readMetaValueAlloc(allocator, path, "server_resolv_mode");
    defer allocator.free(cache_resolv_mode);
    const cache_resolver = try readMetaValueAlloc(allocator, path, "server_resolver");
    defer allocator.free(cache_resolver);
    const cache_node_config_ts = try readMetaValueAlloc(allocator, path, "node_config_ts");
    defer allocator.free(cache_node_config_ts);
    const cache_xray_count = try readMetaValueAlloc(allocator, path, "xray_count");
    defer allocator.free(cache_xray_count);
    const cache_xray_ids_md5 = try readMetaValueAlloc(allocator, path, "xray_ids_md5");
    defer allocator.free(cache_xray_ids_md5);

    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();
    const tfo = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_tfo"));
    defer allocator.free(tfo);
    const server_resolver = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv"));
    defer allocator.free(server_resolver);
    const resolv_mode_raw = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv_mode"));
    defer allocator.free(resolv_mode_raw);
    const node_config_ts = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_config_ts"));
    defer allocator.free(node_config_ts);

    const ids_blob = try joinIdsWithNewlineAlloc(allocator, ids);
    defer allocator.free(ids_blob);
    const ids_md5 = try md5Hex32Alloc(allocator, ids_blob);
    defer allocator.free(ids_md5);
    const current_linux_ver = try linuxVerStringAlloc(allocator);
    defer allocator.free(current_linux_ver);
    const expected_resolv_mode = if (std.mem.eql(u8, resolv_mode_raw, "2")) "2" else "1";
    const expected_tfo = if (tfo.len > 0) tfo else "0";
    const expected_resolver = if (server_resolver.len > 0) server_resolver else "-1";
    const expected_node_config_ts = if (node_config_ts.len > 0) node_config_ts else "0";
    const expected_count = try std.fmt.allocPrint(allocator, "{d}", .{ids.len});
    defer allocator.free(expected_count);

    return std.mem.eql(u8, cache_rev, "1") and
        std.mem.eql(u8, gen_rev, "20260326_6") and
        std.mem.eql(u8, linux_ver, current_linux_ver) and
        std.mem.eql(u8, cache_tfo, expected_tfo) and
        std.mem.eql(u8, cache_resolv_mode, expected_resolv_mode) and
        std.mem.eql(u8, cache_resolver, expected_resolver) and
        std.mem.eql(u8, cache_node_config_ts, expected_node_config_ts) and
        std.mem.eql(u8, cache_xray_count, expected_count) and
        std.mem.eql(u8, cache_xray_ids_md5, ids_md5);
}

fn collectMissingWebtestIds(allocator: std.mem.Allocator, node_dir: []const u8, meta_dir: []const u8, nodes: []const NodeRecord, ids: []const []const u8, out_path: []const u8) !void {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);

    for (ids) |node_id| {
        const idx = findNodeIndexById(nodes, node_id) orelse continue;
        const node = nodes[idx];
        const out_json_path = try std.fmt.allocPrint(allocator, "{s}/{s}_outbounds.json", .{ node_dir, node_id });
        defer allocator.free(out_json_path);
        const meta_path = try std.fmt.allocPrint(allocator, "{s}/{s}.meta", .{ meta_dir, node_id });
        defer allocator.free(meta_path);
        if (fileExists(out_json_path) and fileExists(meta_path)) {
            const cached_rev = try readMetaValueAlloc(allocator, meta_path, "node_rev");
            defer allocator.free(cached_rev);
            const current_rev = try std.fmt.allocPrint(allocator, "{d}", .{node.rev});
            defer allocator.free(current_rev);
            if (std.mem.eql(u8, cached_rev, current_rev)) continue;
        }
        try writer.writeAll(node_id);
        try writer.writeByte('\n');
    }
    try writeFileAtomic(out_path, out.items);
}

fn buildWebtestNodeArtifactsWithShell(allocator: std.mem.Allocator, ids_file: []const u8, nodes: []const NodeRecord, node_dir: []const u8, meta_dir: []const u8, runtime_cfg: WebtestRuntimeConfig) !void {
    const shell_ids_file = try std.fmt.allocPrint(allocator, "/tmp/node_tool_webtest_shell_ids_{d}.txt", .{std.time.microTimestamp()});
    defer {
        std.fs.cwd().deleteFile(shell_ids_file) catch {};
        allocator.free(shell_ids_file);
    }
    var shell_ids = std.ArrayList(u8){};
    defer shell_ids.deinit(allocator);

    const ids_content = try readFileAllocCompat(allocator, ids_file, max_skipd_frame);
    defer allocator.free(ids_content);
    var lines = std.mem.splitScalar(u8, ids_content, '\n');
    while (lines.next()) |raw_line| {
        const node_id = std.mem.trim(u8, raw_line, " \t\r\n");
        if (node_id.len == 0) continue;
        if (findNodeIndexById(nodes, node_id)) |idx| {
            if (try buildWebtestNodeArtifactNative(allocator, nodes[idx], node_dir, meta_dir, runtime_cfg)) {
                continue;
            }
        }
        try shell_ids.appendSlice(allocator, node_id);
        try shell_ids.append(allocator, '\n');
    }
    if (std.mem.trim(u8, shell_ids.items, " \t\r\n").len == 0) return;
    try writeFileAtomic(shell_ids_file, shell_ids.items);

    const script =
        "export KSROOT=/koolshare\n" ++
        ". /koolshare/scripts/ss_base.sh\n" ++
        ". /koolshare/scripts/ss_webtest.sh >/dev/null 2>&1\n" ++
        "WT_NODE_CACHE_DIR=\"$FSS_NODE_JSON_CACHE_DIR\"\n" ++
        "WT_NODE_ENV_DIR=\"$FSS_NODE_ENV_CACHE_DIR\"\n" ++
        "wt_webtest_cache_prepare_dirs || exit 1\n" ++
        "wt_init_reserved_ports\n" ++
        "wt_reset_active_node_env\n" ++
        "WT_CACHE_START_PORT_MAP_FILE=\"/tmp/node_tool_cache_start_ports.$$\"\n" ++
        "wt_assign_webtest_cache_start_ports \"$1\" >/dev/null 2>&1 || exit 1\n" ++
        "worker_threads=$(wt_get_cache_build_threads)\n" ++
        "printf '%s' \"$worker_threads\" | grep -Eq '^[0-9]+$' || worker_threads=1\n" ++
        "[ \"$worker_threads\" -gt 0 ] || worker_threads=1\n" ++
        "worker_fifo=\"/tmp/node_tool_cache_fifo.$$\"\n" ++
        "fail_file=\"/tmp/node_tool_cache_fail.$$\"\n" ++
        "rm -f \"$fail_file\"\n" ++
        "wt_open_fifo_pool \"$worker_threads\" \"$worker_fifo\"\n" ++
        "worker_pids=\"\"\n" ++
        "while IFS= read -r node_id; do\n" ++
        "  [ -n \"$node_id\" ] || continue\n" ++
        "  read -r _ <&3\n" ++
        "  {\n" ++
        "    trap 'echo >&3' EXIT\n" ++
        "    wt_webtest_cache_build_node \"$node_id\" >/dev/null 2>&1 || {\n" ++
        "      echo \"$node_id\" >> \"$fail_file\"\n" ++
        "      fss_clear_webtest_cache_node \"$node_id\" >/dev/null 2>&1\n" ++
        "    }\n" ++
        "  } &\n" ++
        "  worker_pids=\"$worker_pids $!\"\n" ++
        "done < \"$1\"\n" ++
        "[ -n \"$worker_pids\" ] && wait $worker_pids\n" ++
        "wt_close_fifo_pool\n" ++
        "rm -f \"$WT_CACHE_START_PORT_MAP_FILE\"\n" ++
        "WT_CACHE_START_PORT_MAP_FILE=\"\"\n" ++
        "[ ! -s \"$fail_file\" ] || exit 1\n" ++
        "rm -f \"$fail_file\"\n";

    const result = try std.process.Child.run(.{
        .allocator = allocator,
        .argv = &.{ "sh", "-c", script, "sh", shell_ids_file },
        .max_output_bytes = 64 * 1024,
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (result.term != .Exited or result.term.Exited != 0) {
        return error.ChildProcessFailed;
    }
}

fn loadWebtestRuntimeConfig(allocator: std.mem.Allocator, socket_path: []const u8) !WebtestRuntimeConfig {
    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();
    const tfo = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_tfo"));
    const server_resolver = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv"));
    defer if (server_resolver.len == 0) allocator.free(server_resolver);
    const resolv_mode_raw = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv_mode"));
    defer allocator.free(resolv_mode_raw);
    const linux_ver = try linuxVerStringAlloc(allocator);
    return .{
        .linux_ver = linux_ver,
        .tfo = tfo,
        .resolv_mode = if (std.mem.eql(u8, resolv_mode_raw, "2")) try allocator.dupe(u8, "2") else try allocator.dupe(u8, "1"),
        .resolver = if (server_resolver.len > 0) server_resolver else try allocator.dupe(u8, "-1"),
    };
}

fn buildWebtestNodeArtifactNative(allocator: std.mem.Allocator, node: NodeRecord, node_dir: []const u8, meta_dir: []const u8, runtime_cfg: WebtestRuntimeConfig) !bool {
    if (std.mem.eql(u8, node.type_id, "0")) {
        return try buildWebtestSsNative(allocator, node, node_dir, meta_dir, runtime_cfg);
    }
    if (std.mem.eql(u8, node.type_id, "5")) {
        return try buildWebtestTrojanNative(allocator, node, node_dir, meta_dir, runtime_cfg);
    }
    if (std.mem.eql(u8, node.type_id, "8")) {
        return try buildWebtestHy2Native(allocator, node, node_dir, meta_dir, runtime_cfg);
    }
    return false;
}

fn buildWebtestSsNative(allocator: std.mem.Allocator, node: NodeRecord, node_dir: []const u8, meta_dir: []const u8, runtime_cfg: WebtestRuntimeConfig) !bool {
    const ss_obfs = try extractStringFieldAlloc(allocator, node.raw_json, "ss_obfs");
    defer if (ss_obfs) |value| allocator.free(value);
    if (ss_obfs) |value| {
        if (value.len > 0 and !std.mem.eql(u8, value, "0")) return false;
    }
    const method = try extractStringFieldAlloc(allocator, node.raw_json, "method");
    defer if (method) |value| allocator.free(value);
    const password = try extractStringFieldAlloc(allocator, node.raw_json, "password");
    defer if (password) |value| allocator.free(value);
    if (method == null or password == null) return false;

    const out_path = try std.fmt.allocPrint(allocator, "{s}/{s}_outbounds.json", .{ node_dir, node.id });
    defer allocator.free(out_path);
    const meta_path = try std.fmt.allocPrint(allocator, "{s}/{s}.meta", .{ meta_dir, node.id });
    defer allocator.free(meta_path);

    const tcp_fast_open = !std.mem.eql(u8, runtime_cfg.linux_ver, "26") and std.mem.eql(u8, runtime_cfg.tfo, "1");
    const out_json = try std.fmt.allocPrint(allocator,
        "{{\n" ++
            "  \"tag\": \"proxy{s}\",\n" ++
            "  \"protocol\": \"shadowsocks\",\n" ++
            "  \"settings\": {{\n" ++
            "    \"servers\": [{{\n" ++
            "      \"address\": \"{s}\",\n" ++
            "      \"port\": {s},\n" ++
            "      \"password\": \"{s}\",\n" ++
            "      \"method\": \"{s}\",\n" ++
            "      \"uot\": false\n" ++
            "    }}]\n" ++
            "  }},\n" ++
            "  \"streamSettings\": {{\n" ++
            "    \"network\": \"raw\"\n" ++
            "  }},\n" ++
            "  \"sockopt\": {{\n" ++
            "    \"tcpFastOpen\": {s},\n" ++
            "    \"tcpMptcp\": false,\n" ++
            "    \"tcpcongestion\": \"bbr\"\n" ++
            "  }}\n" ++
            "}}\n",
        .{
            node.id,
            node.server,
            node.port,
            password.?,
            method.?,
            if (tcp_fast_open) "true" else "false",
        });
    defer allocator.free(out_json);
    try writeFileAtomic(out_path, out_json);

    const meta = try std.fmt.allocPrint(allocator,
        "node_type=0\n" ++
            "node_rev={d}\n" ++
            "linux_ver={s}\n" ++
            "ss_basic_tfo={s}\n" ++
            "server_resolv_mode={s}\n" ++
            "server_resolver={s}\n" ++
            "has_start=0\n" ++
            "has_stop=0\n" ++
            "start_port=\n" ++
            "built_at={d}\n",
        .{
            node.rev,
            runtime_cfg.linux_ver,
            runtime_cfg.tfo,
            runtime_cfg.resolv_mode,
            runtime_cfg.resolver,
            std.time.timestamp(),
        });
    defer allocator.free(meta);
    try writeFileAtomic(meta_path, meta);
    return true;
}

fn buildWebtestTrojanNative(allocator: std.mem.Allocator, node: NodeRecord, node_dir: []const u8, meta_dir: []const u8, runtime_cfg: WebtestRuntimeConfig) !bool {
    const password = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_uuid");
    defer if (password) |value| allocator.free(value);
    if (password == null or password.?.len == 0) return false;
    const sni = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_sni");
    defer if (sni) |value| allocator.free(value);
    const pcs = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_pcs");
    defer if (pcs) |value| allocator.free(value);
    const vcn = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_vcn");
    defer if (vcn) |value| allocator.free(value);
    const ai = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_ai");
    defer if (ai) |value| allocator.free(value);
    const tfo = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_tfo");
    defer if (tfo) |value| allocator.free(value);
    const plugin = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_plugin");
    defer if (plugin) |value| allocator.free(value);
    const obfs = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_obfs");
    defer if (obfs) |value| allocator.free(value);
    const obfsuri = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_obfsuri");
    defer if (obfsuri) |value| allocator.free(value);
    const obfshost = try extractStringFieldAlloc(allocator, node.raw_json, "trojan_obfshost");
    defer if (obfshost) |value| allocator.free(value);

    const is_ws = plugin != null and obfs != null and std.mem.eql(u8, plugin.?, "obfs-local") and std.mem.eql(u8, obfs.?, "websocket");
    const out_path = try std.fmt.allocPrint(allocator, "{s}/{s}_outbounds.json", .{ node_dir, node.id });
    defer allocator.free(out_path);
    const meta_path = try std.fmt.allocPrint(allocator, "{s}/{s}.meta", .{ meta_dir, node.id });
    defer allocator.free(meta_path);
    const tcp_fast_open = !std.mem.eql(u8, runtime_cfg.linux_ver, "26") and tfo != null and std.mem.eql(u8, tfo.?, "1");
    const allow_insecure = ai != null and std.mem.eql(u8, ai.?, "1");
    const ws_json = if (is_ws)
        try std.fmt.allocPrint(allocator, "{{\"path\":\"{s}\",\"headers\":{{\"Host\":\"{s}\"}}}}", .{ if (obfsuri) |value| value else "", if (obfshost) |value| value else "" })
    else
        try allocator.dupe(u8, "null");
    defer allocator.free(ws_json);
    const out_json = try std.fmt.allocPrint(allocator,
        "{{\n" ++
            "  \"tag\": \"proxy{s}\",\n" ++
            "  \"protocol\": \"trojan\",\n" ++
            "  \"settings\": {{\n" ++
            "    \"servers\": [{{\"address\": \"{s}\", \"port\": {s}, \"password\": \"{s}\"}}]\n" ++
            "  }},\n" ++
            "  \"streamSettings\": {{\n" ++
            "    \"network\": \"{s}\",\n" ++
            "    \"security\": \"tls\",\n" ++
            "    \"tlsSettings\": {{\n" ++
            "      \"serverName\": \"{s}\",\n" ++
            "      \"pinnedPeerCertSha256\": \"{s}\",\n" ++
            "      \"verifyPeerCertByName\": \"{s}\",\n" ++
            "      \"allowInsecure\": {s}\n" ++
            "    }},\n" ++
            "    \"wsSettings\": {s},\n" ++
            "    \"sockopt\": {{\"tcpFastOpen\": {s}}}\n" ++
            "  }}\n" ++
            "}}\n",
        .{
            node.id,
            node.server,
            node.port,
            password.?,
            if (is_ws) "ws" else "tcp",
            if (sni) |value| value else "",
            if (pcs) |value| value else "",
            if (vcn) |value| value else "",
            if (allow_insecure) "true" else "false",
            ws_json,
            if (tcp_fast_open) "true" else "false",
        },
    );
    defer allocator.free(out_json);
    try writeFileAtomic(out_path, out_json);

    const meta = try std.fmt.allocPrint(allocator,
        "node_type=5\nnode_rev={d}\nlinux_ver={s}\nss_basic_tfo={s}\nserver_resolv_mode={s}\nserver_resolver={s}\nhas_start=0\nhas_stop=0\nstart_port=\nbuilt_at={d}\n",
        .{ node.rev, runtime_cfg.linux_ver, runtime_cfg.tfo, runtime_cfg.resolv_mode, runtime_cfg.resolver, std.time.timestamp() },
    );
    defer allocator.free(meta);
    try writeFileAtomic(meta_path, meta);
    return true;
}

fn buildWebtestHy2Native(allocator: std.mem.Allocator, node: NodeRecord, node_dir: []const u8, meta_dir: []const u8, runtime_cfg: WebtestRuntimeConfig) !bool {
    const pass = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_pass");
    defer if (pass) |value| allocator.free(value);
    if (pass == null or pass.?.len == 0) return false;
    const up = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_up");
    defer if (up) |value| allocator.free(value);
    const dl = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_dl");
    defer if (dl) |value| allocator.free(value);
    const obfs = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_obfs");
    defer if (obfs) |value| allocator.free(value);
    const obfs_pass = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_obfs_pass");
    defer if (obfs_pass) |value| allocator.free(value);
    const sni = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_sni");
    defer if (sni) |value| allocator.free(value);
    const pcs = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_pcs");
    defer if (pcs) |value| allocator.free(value);
    const vcn = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_vcn");
    defer if (vcn) |value| allocator.free(value);
    const ai = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_ai");
    defer if (ai) |value| allocator.free(value);
    const tfo = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_tfo");
    defer if (tfo) |value| allocator.free(value);
    const cg = try extractStringFieldAlloc(allocator, node.raw_json, "hy2_cg");
    defer if (cg) |value| allocator.free(value);
    const allow_insecure = ai != null and std.mem.eql(u8, ai.?, "1");
    const tcp_fast_open = !std.mem.eql(u8, runtime_cfg.linux_ver, "26") and tfo != null and std.mem.eql(u8, tfo.?, "1");
    const out_path = try std.fmt.allocPrint(allocator, "{s}/{s}_outbounds.json", .{ node_dir, node.id });
    defer allocator.free(out_path);
    const meta_path = try std.fmt.allocPrint(allocator, "{s}/{s}.meta", .{ meta_dir, node.id });
    defer allocator.free(meta_path);
    const up_value = if (up) |value| try std.fmt.allocPrint(allocator, "{s}mbps", .{value}) else try allocator.dupe(u8, "");
    defer allocator.free(up_value);
    const dl_value = if (dl) |value| try std.fmt.allocPrint(allocator, "{s}mbps", .{value}) else try allocator.dupe(u8, "");
    defer allocator.free(dl_value);
    const finalmask_value = if (obfs != null and std.mem.eql(u8, obfs.?, "1") and obfs_pass != null and obfs_pass.?.len > 0)
        try std.fmt.allocPrint(allocator, ",\n    \"finalmask\": {{\"udp\": [{{\"type\": \"salamander\", \"settings\": {{\"password\": \"{s}\"}}}}]}}", .{obfs_pass.?})
    else
        try allocator.dupe(u8, "");
    defer allocator.free(finalmask_value);
    const out_json = try std.fmt.allocPrint(allocator,
        "{{\n" ++
            "  \"tag\": \"proxy{s}\",\n" ++
            "  \"protocol\": \"hysteria\",\n" ++
            "  \"settings\": {{\"version\": 2, \"address\": \"{s}\", \"port\": {s}}},\n" ++
            "  \"streamSettings\": {{\n" ++
            "    \"network\": \"hysteria\",\n" ++
            "    \"hysteriaSettings\": {{\"version\": 2, \"auth\": \"{s}\", \"congestion\": \"{s}\", \"up\": \"{s}\", \"down\": \"{s}\", \"udphop\": {{\"port\": \"\", \"interval\": 30}}}},\n" ++
            "    \"security\": \"tls\",\n" ++
            "    \"tlsSettings\": {{\"serverName\": \"{s}\", \"pinnedPeerCertSha256\": \"{s}\", \"verifyPeerCertByName\": \"{s}\", \"allowInsecure\": {s}, \"alpn\": [\"h3\"]}},\n" ++
            "    \"sockopt\": {{\"tcpFastOpen\": {s}}}{s}\n" ++
            "  }}\n" ++
            "}}\n",
        .{
            node.id,
            node.server,
            node.port,
            pass.?,
            if (cg) |value| value else "bbr",
            up_value,
            dl_value,
            if (sni) |value| value else node.server,
            if (pcs) |value| value else "",
            if (vcn) |value| value else "",
            if (allow_insecure) "true" else "false",
            if (tcp_fast_open) "true" else "false",
            finalmask_value,
        },
    );
    defer allocator.free(out_json);
    try writeFileAtomic(out_path, out_json);

    const meta = try std.fmt.allocPrint(allocator,
        "node_type=8\nnode_rev={d}\nlinux_ver={s}\nss_basic_tfo={s}\nserver_resolv_mode={s}\nserver_resolver={s}\nhas_start=0\nhas_stop=0\nstart_port=\nbuilt_at={d}\n",
        .{ node.rev, runtime_cfg.linux_ver, runtime_cfg.tfo, runtime_cfg.resolv_mode, runtime_cfg.resolver, std.time.timestamp() },
    );
    defer allocator.free(meta);
    try writeFileAtomic(meta_path, meta);
    return true;
}

fn isWebtestXrayLike(node: NodeRecord) bool {
    return std.mem.eql(u8, node.type_id, "0") or
        std.mem.eql(u8, node.type_id, "3") or
        std.mem.eql(u8, node.type_id, "4") or
        std.mem.eql(u8, node.type_id, "5") or
        std.mem.eql(u8, node.type_id, "8");
}

fn lessThanNumericString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return (std.fmt.parseInt(usize, lhs, 10) catch 0) < (std.fmt.parseInt(usize, rhs, 10) catch 0);
}

fn fileExists(path: []const u8) bool {
    std.fs.cwd().access(path, .{}) catch return false;
    return true;
}

fn readFileAllocCompat(allocator: std.mem.Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    return try readStreamCompatAlloc(allocator, file, max_bytes);
}

fn readMetaValueAlloc(allocator: std.mem.Allocator, path: []const u8, key: []const u8) ![]u8 {
    const content = try readFileAllocCompat(allocator, path, 1024 * 1024);
    defer allocator.free(content);
    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line_raw| {
        const line = std.mem.trim(u8, line_raw, " \t\r\n");
        if (!std.mem.startsWith(u8, line, key)) continue;
        if (line.len <= key.len or line[key.len] != '=') continue;
        return try allocator.dupe(u8, line[key.len + 1 ..]);
    }
    return try allocator.dupe(u8, "");
}

fn writeWebtestGlobalMeta(allocator: std.mem.Allocator, socket_path: []const u8, path: []const u8, ids: []const []const u8) !void {
    var client = try SkipdClient.connect(socket_path);
    defer client.deinit();

    const tfo = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_tfo"));
    defer allocator.free(tfo);
    const server_resolver = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv"));
    defer allocator.free(server_resolver);
    const resolv_mode_raw = try dupOrEmpty(allocator, try client.getAlloc(allocator, "ss_basic_server_resolv_mode"));
    defer allocator.free(resolv_mode_raw);
    const node_config_ts = try dupOrEmpty(allocator, try client.getAlloc(allocator, "fss_node_config_ts"));
    defer allocator.free(node_config_ts);

    const ids_blob = try joinIdsWithNewlineAlloc(allocator, ids);
    defer allocator.free(ids_blob);
    const ids_md5 = try md5Hex32Alloc(allocator, ids_blob);
    defer allocator.free(ids_md5);
    const linux_ver = try linuxVerStringAlloc(allocator);
    defer allocator.free(linux_ver);

    const content = try std.fmt.allocPrint(allocator,
        "cache_rev=1\n" ++
            "gen_rev=20260326_6\n" ++
            "linux_ver={s}\n" ++
            "ss_basic_tfo={s}\n" ++
            "server_resolv_mode={s}\n" ++
            "server_resolver={s}\n" ++
            "node_config_ts={s}\n" ++
            "xray_count={d}\n" ++
            "xray_ids_md5={s}\n" ++
            "built_at={d}\n",
        .{
            linux_ver,
            if (tfo.len > 0) tfo else "0",
            if (std.mem.eql(u8, resolv_mode_raw, "2")) "2" else "1",
            if (server_resolver.len > 0) server_resolver else "-1",
            if (node_config_ts.len > 0) node_config_ts else "0",
            ids.len,
            ids_md5,
            std.time.timestamp(),
        });
    defer allocator.free(content);
    try writeFileAtomic(path, content);
}

fn joinIdsWithNewlineAlloc(allocator: std.mem.Allocator, ids: []const []const u8) ![]u8 {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    for (ids, 0..) |id, idx| {
        if (idx > 0) try out.append(allocator, '\n');
        try out.appendSlice(allocator, id);
    }
    if (ids.len > 0) try out.append(allocator, '\n');
    return try out.toOwnedSlice(allocator);
}

fn md5Hex32Alloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var digest: [16]u8 = undefined;
    std.crypto.hash.Md5.hash(input, &digest, .{});
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    for (digest) |byte| try writer.print("{x:0>2}", .{byte});
    return try out.toOwnedSlice(allocator);
}

fn linuxVerStringAlloc(allocator: std.mem.Allocator) ![]u8 {
    const uts = std.posix.uname();
    const release = std.mem.sliceTo(&uts.release, 0);
    var parts = std.mem.splitScalar(u8, release, '.');
    const p1 = parts.next() orelse "0";
    const p2 = parts.next() orelse "0";
    return try std.fmt.allocPrint(allocator, "{s}{s}", .{ p1, p2 });
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
    if (existing_node) |node| {
        if (std.mem.eql(u8, node.id, assigned_id) and source_override == null and std.mem.eql(u8, raw_json, node.raw_json)) {
            return try allocator.dupe(u8, node.raw_json);
        }
    }

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
    const original_source = try jsonGetStringAlloc(arena, obj, "_source");
    var source = original_source;
    if (source_override) |override| source = override;
    if (source.len == 0) source = "user";
    try normalizeLegacyB64Fields(arena, obj, if (source_override) |override| override else original_source);

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

fn normalizeLegacyB64Fields(arena: std.mem.Allocator, obj: *std.json.ObjectMap, source_hint: []const u8) !void {
    const b64_mode = try jsonGetStringAlloc(arena, obj, "_b64_mode");
    const source = source_hint;
    const should_decode = !std.mem.eql(u8, b64_mode, "raw") and (source.len == 0 or std.mem.eql(u8, source, "subscribe"));
    if (!should_decode) return;

    try maybeDecodeLegacyB64Field(arena, obj, "password", false);
    try maybeDecodeLegacyB64Field(arena, obj, "naive_pass", false);
    try maybeDecodeLegacyB64Field(arena, obj, "v2ray_json", true);
    try maybeDecodeLegacyB64Field(arena, obj, "xray_json", true);
    try maybeDecodeLegacyB64Field(arena, obj, "tuic_json", true);
}

fn maybeDecodeLegacyB64Field(arena: std.mem.Allocator, obj: *std.json.ObjectMap, key: []const u8, compact_json: bool) !void {
    const value = obj.get(key) orelse return;
    if (value != .string) return;
    if (value.string.len == 0) return;

    const decoded = decodeBase64SmartAlloc(arena, value.string) catch return;
    var final_value = decoded;
    if (compact_json) {
        const parsed = std.json.parseFromSlice(std.json.Value, arena, decoded, .{}) catch {
            try obj.put(key, .{ .string = decoded });
            return;
        };
        final_value = try std.json.Stringify.valueAlloc(arena, parsed.value, .{});
    }
    try obj.put(key, .{ .string = final_value });
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

fn extractImportHints(allocator: std.mem.Allocator, raw_json: []const u8) !ImportHints {
    return .{
        .preferred_id = try extractStringFieldAlloc(allocator, raw_json, "_id"),
        .identity = try extractStringFieldAlloc(allocator, raw_json, "_identity"),
        .identity_primary = try extractStringFieldAlloc(allocator, raw_json, "_identity_primary"),
        .identity_secondary = try extractStringFieldAlloc(allocator, raw_json, "_identity_secondary"),
        .source_scope = try extractStringFieldAlloc(allocator, raw_json, "_source_scope"),
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

fn containsSliceOwned(items: [][]u8, value: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, value)) return true;
    }
    return false;
}

fn containsSliceConst(items: []const []const u8, value: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, value)) return true;
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

fn decodeBase64SmartAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const cleaned = try stripWhitespaceAlloc(allocator, input);
    defer allocator.free(cleaned);
    if (cleaned.len == 0) return error.InvalidBase64;

    if (std.mem.indexOfAny(u8, cleaned, "-_")) |_| {
        return decodeBase64Alloc(allocator, cleaned, true);
    }
    return decodeBase64Alloc(allocator, cleaned, false);
}

fn decodeBase64Alloc(allocator: std.mem.Allocator, input: []const u8, url_safe: bool) ![]u8 {
    const padded = try padBase64Alloc(allocator, input);
    defer allocator.free(padded);

    if (url_safe) {
        const converted = try allocator.dupe(u8, padded);
        defer allocator.free(converted);
        for (converted) |*ch| {
            if (ch.* == '-') ch.* = '+';
            if (ch.* == '_') ch.* = '/';
        }
        return decodeBase64StandardAlloc(allocator, converted);
    }
    return decodeBase64StandardAlloc(allocator, padded);
}

fn padBase64Alloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    const pad_len = (4 - (input.len % 4)) % 4;
    var out = try allocator.alloc(u8, input.len + pad_len);
    @memcpy(out[0..input.len], input);
    for (out[input.len..]) |*ch| ch.* = '=';
    return out;
}

fn stripWhitespaceAlloc(allocator: std.mem.Allocator, input: []const u8) ![]u8 {
    var list = std.ArrayList(u8){};
    defer list.deinit(allocator);
    for (input) |ch| {
        if (ch == ' ' or ch == '\t' or ch == '\r' or ch == '\n') continue;
        try list.append(allocator, ch);
    }
    return try list.toOwnedSlice(allocator);
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

fn normalizeGroupLabelAlloc(allocator: std.mem.Allocator, raw_group: []const u8) ![]u8 {
    const trimmed = std.mem.trim(u8, raw_group, " \t\r\n");
    if (trimmed.len == 0) return try allocator.dupe(u8, "");
    if (std.mem.eql(u8, trimmed, "null") or std.mem.eql(u8, trimmed, "_")) {
        return try allocator.dupe(u8, "");
    }
    if (std.mem.lastIndexOfScalar(u8, trimmed, '_')) |pos| {
        if (pos > 0) return try allocator.dupe(u8, trimmed[0..pos]);
    }
    return try allocator.dupe(u8, trimmed);
}

fn sourceBucketKind(node: NodeRecord) []const u8 {
    return if (std.mem.eql(u8, node.source, "subscribe")) "subscribe" else "user";
}

fn sourceBucketKeyAlloc(allocator: std.mem.Allocator, node: NodeRecord) ![]u8 {
    if (!std.mem.eql(u8, node.source, "subscribe")) return try allocator.dupe(u8, "user");
    if (node.source_tag.len > 0) return try allocator.dupe(u8, node.source_tag);
    if (node.source_url_hash.len > 0) return try allocator.dupe(u8, node.source_url_hash);
    if (node.airport_identity.len > 0) return try allocator.dupe(u8, node.airport_identity);
    return try allocator.dupe(u8, "unknown");
}

fn sourceBucketLabelAlloc(allocator: std.mem.Allocator, node: NodeRecord) ![]u8 {
    if (!std.mem.eql(u8, node.source, "subscribe")) return try allocator.dupe(u8, "");
    const label = try normalizeGroupLabelAlloc(allocator, node.group);
    if (label.len > 0) return label;
    allocator.free(label);
    if (node.airport_identity.len > 0) return try allocator.dupe(u8, node.airport_identity);
    if (node.source_tag.len > 0) return try allocator.dupe(u8, node.source_tag);
    return try allocator.dupe(u8, "subscribe");
}

fn findOrCreateSourceBucket(allocator: std.mem.Allocator, buckets: *std.ArrayList(SourceExportBucket), output_dir: []const u8, node: NodeRecord, next_sub_idx: *usize) !usize {
    const source_kind = sourceBucketKind(node);
    const bucket_key = try sourceBucketKeyAlloc(allocator, node);
    defer allocator.free(bucket_key);

    for (buckets.items, 0..) |bucket, idx| {
        if (std.mem.eql(u8, bucket.source, source_kind) and std.mem.eql(u8, bucket.key, bucket_key)) {
            return idx;
        }
    }

    const label = try sourceBucketLabelAlloc(allocator, node);
    errdefer allocator.free(label);
    const key = try allocator.dupe(u8, bucket_key);
    errdefer allocator.free(key);
    const source = try allocator.dupe(u8, source_kind);
    errdefer allocator.free(source);

    const path = if (std.mem.eql(u8, source_kind, "user"))
        try std.fmt.allocPrint(allocator, "{s}/local_0_user.txt", .{output_dir})
    else blk: {
        next_sub_idx.* += 1;
        break :blk try std.fmt.allocPrint(allocator, "{s}/local_{d}_{s}.txt", .{ output_dir, next_sub_idx.*, key });
    };
    errdefer allocator.free(path);

    try buckets.append(allocator, .{
        .source = source,
        .key = key,
        .label = label,
        .path = path,
        .count = 0,
        .content = std.ArrayList(u8){},
    });
    return buckets.items.len - 1;
}

fn findOrCreateWebtestGroup(allocator: std.mem.Allocator, groups: *std.ArrayList(WebtestGroupBucket), output_dir: []const u8, tag: []const u8) !usize {
    for (groups.items, 0..) |group, idx| {
        if (std.mem.eql(u8, group.tag, tag)) return idx;
    }
    const path = try std.fmt.allocPrint(allocator, "{s}/wt_{d}_{s}.txt", .{ output_dir, groups.items.len + 1, tag });
    errdefer allocator.free(path);
    try groups.append(allocator, .{
        .tag = try allocator.dupe(u8, tag),
        .path = path,
        .content = std.ArrayList(u8){},
    });
    return groups.items.len - 1;
}

fn webtestGroupTagAlloc(allocator: std.mem.Allocator, node: NodeRecord) ![]u8 {
    if (std.mem.eql(u8, node.type_id, "0")) {
        const ss_obfs = try extractStringFieldAlloc(allocator, node.raw_json, "ss_obfs");
        defer if (ss_obfs) |value| allocator.free(value);
        const method = try extractStringFieldAlloc(allocator, node.raw_json, "method");
        defer if (method) |value| allocator.free(value);
        const obfs_enable = if (ss_obfs) |value| value.len > 0 and !std.mem.eql(u8, value, "0") else false;
        const is_2022 = if (method) |value| std.mem.indexOf(u8, value, "2022-blake") != null else false;
        if (is_2022) {
            return try allocator.dupe(u8, if (obfs_enable) "00_05" else "00_04");
        }
        return try allocator.dupe(u8, if (obfs_enable) "00_02" else "00_01");
    }
    if (node.type_id.len == 1) return try std.fmt.allocPrint(allocator, "0{s}", .{node.type_id});
    return try allocator.dupe(u8, node.type_id);
}

fn isWebtestXrayLikeTag(tag: []const u8) bool {
    return std.mem.eql(u8, tag, "00_01") or
        std.mem.eql(u8, tag, "00_02") or
        std.mem.eql(u8, tag, "00_04") or
        std.mem.eql(u8, tag, "00_05") or
        std.mem.eql(u8, tag, "03") or
        std.mem.eql(u8, tag, "04") or
        std.mem.eql(u8, tag, "05") or
        std.mem.eql(u8, tag, "08");
}

fn writeSourceBuckets(allocator: std.mem.Allocator, buckets: *std.ArrayList(SourceExportBucket)) !void {
    for (buckets.items) |bucket| {
        try writeFileAtomic(bucket.path, bucket.content.items);
    }
    _ = allocator;
}

fn writeSourceMetaFile(allocator: std.mem.Allocator, meta_path: []const u8, buckets: []const SourceExportBucket) !void {
    var out = std.ArrayList(u8){};
    defer out.deinit(allocator);
    const writer = out.writer(allocator);
    for (buckets) |bucket| {
        try writer.print("{s}\t{d}\t{s}\t{s}\n", .{
            bucket.path,
            bucket.count,
            bucket.key,
            bucket.label,
        });
    }
    try writeFileAtomic(meta_path, out.items);
}

fn writeSourceExportText(writer: anytype, buckets: []const SourceExportBucket, total: usize) !void {
    try writer.print("command: export-sources\n", .{});
    try writer.print("total: {d}\n", .{total});
    try writer.print("files: {d}\n", .{buckets.len});
    for (buckets) |bucket| {
        try writer.print("item: {s}\t{s}\t{d}\t{s}\t{s}\n", .{
            bucket.source,
            bucket.path,
            bucket.count,
            bucket.key,
            bucket.label,
        });
    }
}

fn writeSourceExportShell(writer: anytype, buckets: []const SourceExportBucket, total: usize) !void {
    try writer.print("summary\t{d}\t{d}\n", .{ total, buckets.len });
    for (buckets) |bucket| {
        try writer.print("item\t{s}\t{s}\t{d}\t{s}\t{s}\n", .{
            bucket.source,
            bucket.path,
            bucket.count,
            bucket.key,
            bucket.label,
        });
    }
}

fn writeSourceExportJson(allocator: std.mem.Allocator, writer: anytype, buckets: []const SourceExportBucket, total: usize) !void {
    try writer.print("{{\"command\":\"export-sources\",\"total\":{d},\"files\":[", .{total});
    for (buckets, 0..) |bucket, idx| {
        if (idx > 0) try writer.writeAll(",");
        var out = std.ArrayList(u8){};
        defer out.deinit(allocator);
        const w = out.writer(allocator);
        try w.writeAll("{\"source\":");
        try writeJsonString(w, bucket.source);
        try w.writeAll(",\"path\":");
        try writeJsonString(w, bucket.path);
        try w.print(",\"count\":{d},\"key\":", .{bucket.count});
        try writeJsonString(w, bucket.key);
        try w.writeAll(",\"label\":");
        try writeJsonString(w, bucket.label);
        try w.writeAll("}");
        try writer.writeAll(out.items);
    }
    try writer.writeAll("]}\n");
}

fn writePruneExportText(writer: anytype, kept_count: usize, removed: []const SourceExportBucket) !void {
    try writer.print("command: prune-export-sources\n", .{});
    try writer.print("kept: {d}\n", .{kept_count});
    try writer.print("removed: {d}\n", .{removed.len});
    for (removed) |item| {
        try writer.print("remove: {s}\t{s}\t{d}\t{s}\n", .{
            item.key,
            item.label,
            item.count,
            item.path,
        });
    }
}

fn writePruneExportShell(writer: anytype, kept_count: usize, removed: []const SourceExportBucket) !void {
    try writer.print("summary\t{d}\t{d}\n", .{ kept_count, removed.len });
    for (removed) |item| {
        try writer.print("remove\t{s}\t{s}\t{d}\t{s}\n", .{
            item.key,
            item.label,
            item.count,
            item.path,
        });
    }
}

fn writePruneExportJson(allocator: std.mem.Allocator, writer: anytype, kept_count: usize, removed: []const SourceExportBucket) !void {
    try writer.print("{{\"command\":\"prune-export-sources\",\"kept\":{d},\"removed\":[", .{kept_count});
    for (removed, 0..) |item, idx| {
        if (idx > 0) try writer.writeAll(",");
        var out = std.ArrayList(u8){};
        defer out.deinit(allocator);
        const w = out.writer(allocator);
        try w.writeAll("{\"key\":");
        try writeJsonString(w, item.key);
        try w.writeAll(",\"label\":");
        try writeJsonString(w, item.label);
        try w.print(",\"count\":{d},\"path\":", .{item.count});
        try writeJsonString(w, item.path);
        try w.writeAll("}");
        try writer.writeAll(out.items);
    }
    try writer.writeAll("]}\n");
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
        "  node-tool find-duplicates [options]\n" ++
        "  node-tool export-sources [options]\n" ++
        "  node-tool prune-export-sources [options]\n" ++
        "  node-tool webtest-groups [options]\n" ++
        "  node-tool node2json [options]\n" ++
        "  node-tool sync-source [options]\n" ++
        "  node-tool json2node [options]\n" ++
        "  node-tool add-node [options]\n" ++
        "  node-tool delete-node [options]\n" ++
        "  node-tool delete-nodes [options]\n" ++
        "  node-tool dedupe [options]\n" ++
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
        .find_duplicates => try writer.writeAll("Usage: node-tool find-duplicates [--match identity|config|all] [--format text|json|shell] [--socket path]\n"),
        .export_sources => try writer.writeAll("Usage: node-tool export-sources --output-dir /tmp/fancyss_subs --meta /tmp/fancyss_subs/local_split_meta.tsv [--all-jsonl /tmp/fancyss_subs/ss_nodes_spl.txt] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--group name] [--protocol type] [--name keyword] [--identity value] [--format text|json|shell] [--socket path]\n"),
        .prune_export_sources => try writer.writeAll("Usage: node-tool prune-export-sources --meta /tmp/fancyss_subs/local_split_meta.tsv --active-source-tags /tmp/fancyss_subs/active_source_tags.txt [--format text|json|shell]\n"),
        .webtest_groups => try writer.writeAll("Usage: node-tool webtest-groups --output-dir /tmp/fancyss_webtest [--ids 1,2,3 | --ids-file /tmp/ids.txt | --source user|subscribe | --source-tag tag | --airport-identity value | --group name | --protocol type | --name keyword | --identity value] [--socket path]\n"),
        .node2json => try writer.writeAll("Usage: node-tool node2json [--schema2] [--ids 1,2,3] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--name keyword] [--identity value] [--format json|jsonl] [--canonical] [--socket path]\n"),
        .sync_source => try writer.writeAll("Usage: node-tool sync-source --source-tag tag [--input nodes.jsonl | --stdin] [--reuse-ids] [--normalized-output file] [--plan-output file] [--plan-format shell|json|text] [--dry-run] [--socket path]\n"),
        .json2node => try writer.writeAll("Usage: node-tool json2node [--input nodes.jsonl | --stdin] [--mode append|replace] [--reuse-ids] [--source user|subscribe] [--normalized-output file] [--plan-output file] [--plan-format shell|json|text] [--dry-run] [--socket path]\n"),
        .add_node => try writer.writeAll("Usage: node-tool add-node [--input node.json | --stdin] [--position tail|head|before:<id>|after:<id>] [--source user|subscribe] [--dry-run] [--socket path]\n"),
        .delete_node => try writer.writeAll("Usage: node-tool delete-node [--ids 23 | --identity xxx_yyy | --name keyword] [--dry-run] [--socket path]\n"),
        .delete_nodes => try writer.writeAll("Usage: node-tool delete-nodes [--ids 1,2,3 | --identity xxx_yyy | --name keyword | --source-tag tag | --source user|subscribe | --group name | --airport-identity value | --protocol type | --all-subscribe | --all] [--dry-run] [--socket path]\n"),
        .dedupe => try writer.writeAll("Usage: node-tool dedupe [--match identity|config|all] [--dry-run] [--socket path]\n"),
        .warm_cache => try writer.writeAll("Usage: node-tool warm-cache [--env] [--json] [--direct-domains] [--webtest] [--ids 1,2,3 | --ids-file /tmp/ids.txt] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--group name] [--protocol type] [--name keyword] [--identity value] [--socket path]\n"),
        .reorder => try writer.writeAll("Usage: node-tool reorder [--ids 5,3,1 | --sort name|created|updated|id|protocol] [--source user|subscribe] [--source-tag tag] [--airport-identity value] [--group name] [--protocol type] [--name keyword] [--identity value] [--dry-run] [--socket path]\n"),
        .plan => try writer.writeAll("Usage: node-tool plan [--input nodes.jsonl | --stdin] [--mode append|replace] [--reuse-ids] [--source user|subscribe] [--format json|text|shell] [--socket path]\n"),
        .version => try writer.writeAll("Usage: node-tool version\n"),
    }
}

test "parse version command" {
    const args = [_][]const u8{ "node-tool", "version" };
    const options = try parseArgs(&args);
    try std.testing.expect(options.command == .version);
}

test "parse export-sources command" {
    const args = [_][]const u8{
        "node-tool",
        "export-sources",
        "--output-dir",
        "/tmp/fancyss_subs",
        "--meta",
        "/tmp/fancyss_subs/local_split_meta.tsv",
        "--all-jsonl",
        "/tmp/fancyss_subs/ss_nodes_spl.txt",
    };
    const options = try parseArgs(&args);
    try std.testing.expect(options.command == .export_sources);
    try std.testing.expect(std.mem.eql(u8, options.query.output_dir.?, "/tmp/fancyss_subs"));
    try std.testing.expect(std.mem.eql(u8, options.query.meta_path.?, "/tmp/fancyss_subs/local_split_meta.tsv"));
    try std.testing.expect(std.mem.eql(u8, options.query.all_jsonl_path.?, "/tmp/fancyss_subs/ss_nodes_spl.txt"));
}

test "parse json2node plan output args" {
    const args = [_][]const u8{
        "node-tool",
        "json2node",
        "--input",
        "/tmp/nodes.jsonl",
        "--mode",
        "replace",
        "--reuse-ids",
        "--normalized-output",
        "/tmp/nodes.normalized.jsonl",
        "--plan-output",
        "/tmp/plan.tsv",
        "--plan-format",
        "shell",
        "--dry-run",
    };
    const options = try parseArgs(&args);
    try std.testing.expect(options.command == .json2node);
    try std.testing.expect(std.mem.eql(u8, options.query.normalized_output.?, "/tmp/nodes.normalized.jsonl"));
    try std.testing.expect(std.mem.eql(u8, options.query.plan_output.?, "/tmp/plan.tsv"));
    try std.testing.expect(std.mem.eql(u8, options.query.plan_format.?, "shell"));
}

test "parse prune export sources args" {
    const args = [_][]const u8{
        "node-tool",
        "prune-export-sources",
        "--meta",
        "/tmp/fancyss_subs/local_split_meta.tsv",
        "--active-source-tags",
        "/tmp/fancyss_subs/active_source_tags.txt",
        "--format",
        "shell",
    };
    const options = try parseArgs(&args);
    try std.testing.expect(options.command == .prune_export_sources);
    try std.testing.expect(std.mem.eql(u8, options.query.meta_path.?, "/tmp/fancyss_subs/local_split_meta.tsv"));
    try std.testing.expect(std.mem.eql(u8, options.query.active_source_tags_path.?, "/tmp/fancyss_subs/active_source_tags.txt"));
}

test "parse skipd list body" {
    const body = "list fss_node_order 1,2,3\n";
    const parsed = try parseSkipdBody(body);
    try std.testing.expect(std.mem.eql(u8, parsed.command, "list"));
    try std.testing.expect(std.mem.eql(u8, parsed.key, "fss_node_order"));
    try std.testing.expect(std.mem.eql(u8, parsed.value, "1,2,3"));
}
