g++ -O3 -shared -std=c++20 -fPIC \
    $(python3 -m pybind11 --includes) \
    gobackend.cpp \
    -o gobackend$(python3-config --extension-suffix)
