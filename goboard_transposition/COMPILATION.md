# Default (go_transposition)
meson setup builddir
meson compile -C builddir

# Override
meson setup builddir -Dmodule_name=gobackend
meson compile -C builddir

# Or reconfigure an existing tree
meson configure builddir -Dmodule_name=go_transposition
meson compile -C builddir
