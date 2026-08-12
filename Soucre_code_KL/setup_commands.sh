#!/bin/bash
# bien dich va sinh du lieu TPC-H (Scale Factor 100)

echo "Bat dau qua trinh sinh du lieu TPC-H ..."

# Di chuyen vao thu muc dbgen
cd tpch-dbgen

# Bien dich ma nguon
make

# Sinh du lieu voi Scale Factor = 100
./dbgen -s 100 -f

echo "Da sinh du lieu thanh cong!"