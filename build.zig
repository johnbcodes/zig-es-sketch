const std = @import("std");

// Although this function looks imperative, note that its job is to
// declaratively construct a build graph that will be executed by an external
// runner.
pub fn build(b: *std.Build) void {
    // Standard target options allows the person running `zig build` to choose
    // what target to build for. Here we do not override the defaults, which
    // means any target is allowed, and the default is native. Other options
    // for restricting supported target set are available.
    const target = b.standardTargetOptions(.{});

    // Standard optimization options allow the person running `zig build` to select
    // between Debug, ReleaseSafe, ReleaseFast, and ReleaseSmall. Here we do not
    // set a preferred release mode, allowing the user to decide how to optimize.
    const optimize = b.standardOptimizeOption(.{});

    // Add Zig SQLite3 library dependency
    const sqlite = b.dependency("sqlite", .{
        .target = target,
        .optimize = optimize,
    });
    const uuid6 = b.dependency("uuid6", .{
        .target = target,
        .optimize = optimize,
    });

    const lib = b.addStaticLibrary(.{
        .name = "zig-es-sketch",
        // In this case the main source file is merely a path, however, in more
        // complicated build scripts, this could be a generated file.
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    lib.addModule("sqlite", sqlite.module("sqlite"));
    lib.addModule("uuid6", uuid6.module("uuid6"));

    // This declares intent for the library to be installed into the standard
    // location when the user invokes the "install" step (the default step when
    // running `zig build`).
    b.installArtifact(lib);

    // Creates a step for unit testing. This only builds the test executable
    // but does not run it.
    const main_tests = b.addTest(.{
        .root_source_file = .{ .path = "src/main.zig" },
        .target = target,
        .optimize = optimize,
    });
    main_tests.addModule("sqlite", sqlite.module("sqlite"));
    main_tests.addModule("uuid6", uuid6.module("uuid6"));
    main_tests.addIncludePath(.{ .path = "c" });
    main_tests.linkLibrary(sqlite.artifact("sqlite"));

    const run_main_tests = b.addRunArtifact(main_tests);

    // This creates a build step. It will be visible in the `zig build --help` menu,
    // and can be selected like this: `zig build test`
    // This will evaluate the `test` step rather than the default, which is "install".
    const test_step = b.step("test", "Run library tests");
    test_step.dependOn(&run_main_tests.step);

    // CommandHandler POC run step
    const poc_exe = b.addExecutable(.{
        .name = "command-handler-poc",
        .root_source_file = .{ .path = "src/command_handler_poc.zig" },
        .target = target,
        .optimize = optimize,
    });
    poc_exe.addModule("sqlite", sqlite.module("sqlite"));
    poc_exe.addModule("uuid6", uuid6.module("uuid6"));
    poc_exe.addIncludePath(.{ .path = "c" });
    poc_exe.linkLibrary(sqlite.artifact("sqlite"));
    b.installArtifact(poc_exe);
    const poc_run_cmd = b.addRunArtifact(poc_exe);
    poc_run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        poc_run_cmd.addArgs(args);
    }
    const poc_run_step = b.step("run-ch-poc", "Run the command handler POC");
    poc_run_step.dependOn(&poc_run_cmd.step);
}
