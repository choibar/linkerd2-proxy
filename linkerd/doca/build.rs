use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

// The DOCA transport lives in the parent repo at src/transport and is built
// there by meson (`ninja -C src/transport/build`) into static archives:
// libdmesh_dpu.a + libdmesh_common.a + libdmesh_host.a (the C datapath) and
// device/dpa_kernel.a (the dpacc output). This script compiles only the shim and links those
// archives, so the transport's source list exists in one place
// (src/transport/meson.build).
const TRANSPORT: &str = "../../../src/transport";

fn main() {
    let transport = PathBuf::from(TRANSPORT);
    let build_dir = transport.join("build");

    println!("cargo:rerun-if-changed=src/shim.c");
    for dir in ["common", "dpu", "host"] {
        println!("cargo:rerun-if-changed={}", transport.join(dir).display());
    }
    for archive in [
        "libdmesh_dpu.a",
        "libdmesh_common.a",
        "libdmesh_host.a",
        "device/dpa_kernel.a",
    ] {
        println!("cargo:rerun-if-changed={}", build_dir.join(archive).display());
    }

    let libs = [
        "doca-common",
        "doca-comch",
        "doca-dma",
        "doca-aes-gcm",
        "doca-sha",
        "doca-dpa",
        "libflexio",
    ];

    let mut include_paths = BTreeSet::new();
    for lib in libs {
        let found = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(lib)
            .unwrap_or_else(|error| panic!("failed to find {lib} with pkg-config: {error}"));
        include_paths.extend(found.include_paths);
    }

    let mut build = cc::Build::new();
    build
        .file("src/shim.c")
        .flag_if_supported("-Wno-deprecated-declarations")
        .define("ALLOW_EXPERIMENTAL_API", None)
        .define("DOCA_ALLOW_EXPERIMENTAL_API", None)
        .define("FLEXIO_ALLOW_EXPERIMENTAL_API", None);

    for path in include_paths {
        build.include(path);
    }
    for dir in ["common", "dpu", "host"] {
        build.include(transport.join(dir));
    }

    build.compile("dmesh_doca_shim");

    // Copy the archives next to the shim so one link-search path covers them;
    // dpacc names its output dpa_kernel.a (no lib prefix), which rustc's
    // `-l static=` cannot find in place, and meson emits *thin* archives whose
    // members are paths relative to the build dir, which rustc cannot bundle
    // into the rlib - those are re-packed as regular archives.
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set"));
    for (source, target) in [
        ("libdmesh_dpu.a", "libdmesh_dpu.a"),
        ("libdmesh_common.a", "libdmesh_common.a"),
        ("libdmesh_host.a", "libdmesh_host.a"),
        ("device/dpa_kernel.a", "libdpa_kernel.a"),
    ] {
        let from = build_dir.join(source).canonicalize().unwrap_or_else(|error| {
            panic!(
                "failed to find transport archive {source}: {error} \
                 (run `ninja -C src/transport/build` in the parent repo first)"
            )
        });
        copy_archive(&from, &out_dir.join(target));
    }
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    // All four go on the final link as whole archives: dpu and common reference
    // each other both ways (object.c -> dma.c and back), and a plain `static=`
    // library is bundled into the rlib, which the linker scans *before* the
    // whole-archive objects that need it (client_send_msg, DPU_mesh_dpa_app).
    println!("cargo:rustc-link-lib=static:+whole-archive=dmesh_dpu");
    println!("cargo:rustc-link-lib=static:+whole-archive=dmesh_common");
    println!("cargo:rustc-link-lib=static:+whole-archive=dmesh_host");
    println!("cargo:rustc-link-lib=static:+whole-archive=dpa_kernel");
    // SDK libraries must follow the archives that reference them. Emitting
    // these first lets --as-needed discard constructor-only dependencies in
    // standalone crate tests (DOCA logging and the dpacc registration stubs).
    for lib in libs {
        pkg_config::Config::new()
            .probe(lib)
            .unwrap_or_else(|error| panic!("failed to find {lib} with pkg-config: {error}"));
    }
}

/// Copies a static archive, re-packing a thin archive (`!<thin>` magic; its
/// members are stored as paths relative to the archive's directory) into a
/// regular one so rustc can bundle it.
fn copy_archive(from: &Path, to: &Path) {
    let magic = fs::read(from).unwrap_or_else(|error| panic!("failed to read {}: {error}", from.display()));
    if !magic.starts_with(b"!<thin>\n") {
        fs::copy(from, to)
            .unwrap_or_else(|error| panic!("failed to copy {} for linking: {error}", from.display()));
        return;
    }
    let base = from.parent().expect("archive path has a parent");
    let listing = Command::new("ar")
        .arg("t")
        .arg(from)
        .output()
        .unwrap_or_else(|error| panic!("failed to run `ar t {}`: {error}", from.display()));
    assert!(listing.status.success(), "`ar t {}` failed", from.display());
    let members: Vec<PathBuf> = String::from_utf8_lossy(&listing.stdout)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|member| base.join(member))
        .collect();
    let _ = fs::remove_file(to);
    let status = Command::new("ar")
        .arg("crs")
        .arg(to)
        .args(&members)
        .status()
        .unwrap_or_else(|error| panic!("failed to run `ar crs {}`: {error}", to.display()));
    assert!(status.success(), "re-packing {} into {} failed", from.display(), to.display());
}
