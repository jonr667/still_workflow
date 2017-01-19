#! /bin/bash
set -e
plot_uv.py -a autos -t 1 --plot_each=time --pretty --xlim 0_1023 --max=2 --drng=8 -o ${1}.autos.png $1
