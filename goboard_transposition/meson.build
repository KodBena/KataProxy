project(
  'go-transposition',
  'cpp',
  version: '0.1.0',
  meson_version: '>=1.1.0',
  default_options: [
    'cpp_std=c++20',
    'optimization=3',
    # NOTE: -march=native is deliberately NOT set here.
    # It would produce non-portable binaries that crash with SIGILL on
    # user machines with different CPU features.
    # For a local developer build with native tuning:
    #     meson configure builddir -Dcpp_args=-march=native
  ],
)

module_name = get_option('module_name')
message('Building pybind11 extension as: ' + module_name)

# --- Python ---
py = import('python').find_installation(pure: false)

# --- pybind11 ---
# Try pkg-config/cmake first (installed via pip installs a pybind11Config.cmake),
# then fall back to asking pybind11 directly for its include path.
pybind11_dep = dependency('pybind11', required: false)
if not pybind11_dep.found()
  _pybind11_inc = run_command(
    py, '-c', 'import pybind11; print(pybind11.get_include())',
    check: true,
  ).stdout().strip()
  message('pybind11 not found via pkg-config; using include path: ' + _pybind11_inc)
  pybind11_dep = declare_dependency(
    include_directories: include_directories(_pybind11_inc),
  )
endif

# --- nlohmann/json ---
# Vendored as a single-header amalgamation in third_party/nlohmann/json.hpp.
# This guarantees cross-platform reproducibility and eliminates any
# dependence on distro package managers or external package registries
# during the build.
# See third_party/README.md for license attribution and update procedure.
nlohmann_dep = declare_dependency(
  include_directories: include_directories('third_party'),
)

# --- Extension ---
py.extension_module(
  module_name,
  'gobackend.cpp',
  dependencies: [
    py.dependency(),
    pybind11_dep,
    nlohmann_dep,
  ],
  cpp_args: [
    # This is what allows PYBIND11_MODULE(MODULE_NAME, m) to work.
    '-DMODULE_NAME=' + module_name,
  ],
  # goboard.h lives next to gobackend.cpp; add extra paths here if needed.
  include_directories: include_directories('.'),
  install: true,
  install_dir: py.get_install_dir(),
)
