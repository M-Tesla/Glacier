{
  "targets": [
    {
      "target_name": "glacier",
      "sources": ["src/glacier_napi.c"],
      "include_dirs": ["../../include"],
      "libraries": ["-LRelease", "-lglacier"],
      "ldflags": ["-Wl,-rpath=\\$$ORIGIN"],
      "copies": [
        {
          "destination": "<(PRODUCT_DIR)",
          "files": ["<(module_root_dir)/../../zig-out/lib/libglacier.so"]
        }
      ]
    }
  ]
}
