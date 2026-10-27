use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=csrc/spdk_shim.c");
    println!("cargo:rerun-if-env-changed=VALS_SPDK_LINK");
    println!("cargo:rerun-if-env-changed=VALS_SPDK_DIR");

    if std::env::var_os("CARGO_FEATURE_SPDK").is_none() {
        return;
    }
    if std::env::var_os("VALS_SPDK_LINK").is_none() {
        println!(
            "cargo:warning=spdk feature on but VALS_SPDK_LINK unset: not linking SPDK \
             (module type-checks; NVMe calls won't link). Set VALS_SPDK_LINK=1 on a built box."
        );
        return;
    }

    let manifest = env!("CARGO_MANIFEST_DIR");
    let spdk_dir = std::env::var("VALS_SPDK_DIR")
        .unwrap_or_else(|_| format!("{manifest}/../../third_party/spdk"));
    let include = format!("{spdk_dir}/build/include");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");

    assert!(
        Path::new(&include).join("spdk/nvme.h").exists(),
        "SPDK headers not found at {include} — build SPDK first (see docs/SPDK.md)"
    );

    let shim_o = format!("{out_dir}/spdk_shim.o");
    let shim_a = format!("{out_dir}/libvalsshim.a");
    run(Command::new("cc").args([
        "-fPIC",
        "-O2",
        "-c",
        &format!("{manifest}/csrc/spdk_shim.c"),
        "-I",
        &include,
        "-o",
        &shim_o,
    ]));
    let _ = std::fs::remove_file(&shim_a);
    run(Command::new("ar").args(["crs", &shim_a, &shim_o]));
    println!("cargo:rustc-link-search=native={out_dir}");
    println!("cargo:rustc-link-lib=static=valsshim");

    let pc_path = format!("{spdk_dir}/build/lib/pkgconfig:{spdk_dir}/dpdk/build/lib/pkgconfig");
    let out = Command::new("pkg-config")
        .env("PKG_CONFIG_PATH", &pc_path)
        .args([
            "--libs",
            "--static",
            "spdk_nvme",
            "spdk_env_dpdk",
            "spdk_syslibs",
        ])
        .output()
        .expect("run pkg-config for SPDK");
    assert!(
        out.status.success(),
        "pkg-config failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    for tok in String::from_utf8_lossy(&out.stdout).split_whitespace() {
        if let Some(p) = tok.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={p}");
        } else if let Some(name) = tok.strip_prefix("-l") {
            if name.starts_with("spdk_") {
                println!("cargo:rustc-link-lib=static:+whole-archive={name}");
            } else {
                println!("cargo:rustc-link-lib={name}");
            }
        } else if tok.ends_with(".a") {
            let path = Path::new(tok);
            if let (Some(dir), Some(stem)) =
                (path.parent(), path.file_stem().and_then(|s| s.to_str()))
            {
                let lib = stem.strip_prefix("lib").unwrap_or(stem);
                println!("cargo:rustc-link-search=native={}", dir.display());
                println!("cargo:rustc-link-lib=static={lib}");
            }
        }
    }
    println!("cargo:rustc-link-search=native={spdk_dir}/build/lib");
    println!("cargo:rustc-link-search=native={spdk_dir}/dpdk/build/lib");
}

fn run(cmd: &mut Command) {
    let status = cmd.status().expect("spawn build command");
    assert!(status.success(), "command failed: {cmd:?}");
}
