const std = @import("std");

const app_version = "0.1.0";

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

const Options = struct {
    command: Command,
    passthrough_args: []const []const u8 = &.{},
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
        .list => try runStub("list", options.passthrough_args),
        .stat => try runStub("stat", options.passthrough_args),
        .find => try runStub("find", options.passthrough_args),
        .node2json => try runStub("node2json", options.passthrough_args),
        .json2node => try runStub("json2node", options.passthrough_args),
        .add_node => try runStub("add-node", options.passthrough_args),
        .delete_node => try runStub("delete-node", options.passthrough_args),
        .delete_nodes => try runStub("delete-nodes", options.passthrough_args),
        .warm_cache => try runStub("warm-cache", options.passthrough_args),
        .reorder => try runStub("reorder", options.passthrough_args),
        .plan => try runStub("plan", options.passthrough_args),
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

    if (args.len >= 3) {
        if (std.mem.eql(u8, args[2], "-h") or std.mem.eql(u8, args[2], "--help")) {
            try printCommandUsage(command, std.fs.File.stdout().deprecatedWriter());
            std.process.exit(0);
        }
    }

    return .{
        .command = command,
        .passthrough_args = if (args.len > 2) args[2..] else &.{},
    };
}

fn runStub(command_name: []const u8, passthrough_args: []const []const u8) !void {
    const stdout = std.fs.File.stdout().deprecatedWriter();
    try stdout.print("node-tool: {s} is planned but not implemented yet\n", .{command_name});
    if (passthrough_args.len > 0) {
        try stdout.writeAll("args:");
        for (passthrough_args) |arg| {
            try stdout.print(" [{s}]", .{arg});
        }
        try stdout.writeAll("\n");
    }
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
        .list => try writer.writeAll("Usage: node-tool list [--source-tag tag] [--group name] [--protocol type] [--format table|json|jsonl]\n"),
        .stat => try writer.writeAll("Usage: node-tool stat [--format text|json]\n"),
        .find => try writer.writeAll("Usage: node-tool find [--name value] [--identity value] [--source-tag tag] [--airport-identity value]\n"),
        .node2json => try writer.writeAll("Usage: node-tool node2json [--schema2] [--ids 1,2,3] [--source user|subscribe] [--source-tag tag] [--format json|jsonl] [--canonical]\n"),
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
