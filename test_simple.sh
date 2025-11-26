#!/bin/bash

BINARY="./no_sql_dbms"
DB="testdb"

echo "Building..."
cd cmd/no_sql_dbms
go build -o ../../no_sql_dbms .
cd ../..

# чистим старые данные
rm -rf data
mkdir -p data

echo "Starting tests"
echo ""

echo "=== INSERT ==="
$BINARY $DB insert '{"name":"Alice","age":25,"city":"London"}'
$BINARY $DB insert '{"name":"Bob","age":30,"city":"Paris"}'
$BINARY $DB insert '{"name":"Carol","age":22,"city":"London"}'
$BINARY $DB insert '{"name":"David","age":35,"city":"Berlin"}'
$BINARY $DB insert '{"name":"Eve","age":28,"city":"Paris"}'
echo ""

# простые запросы
echo "=== FIND by age ==="
$BINARY $DB find '{"age":25}'
echo ""

echo "=== FIND by city ==="
$BINARY $DB find '{"city":"Paris"}'
echo ""

# неявный AND
echo "=== FIND with implicit AND ==="
$BINARY $DB find '{"name":"Alice","city":"London"}'
echo ""

# OR оператор
echo "=== FIND with OR ==="
$BINARY $DB find '{"$or":[{"age":25},{"city":"Paris"}]}'
echo ""

# GT оператор
echo "=== FIND with GT (age > 30) ==="
$BINARY $DB find '{"age":{"$gt":30}}'
echo ""

# LT оператор
echo "=== FIND with LT (age < 26) ==="
$BINARY $DB find '{"age":{"$lt":26}}'
echo ""

# LIKE оператор
echo "=== FIND with LIKE (name starts with Al) ==="
$BINARY $DB find '{"name":{"$like":"Al%"}}'
echo ""

echo "=== FIND with LIKE (wildcard in middle) ==="
$BINARY $DB find '{"name":{"$like":"C_rol"}}'
echo ""

# IN оператор
echo "=== FIND with IN ==="
$BINARY $DB find '{"city":{"$in":["London","Paris"]}}'
echo ""

# сложные запросы
echo "=== Complex query (age > 25 AND city = Paris) ==="
$BINARY $DB find '{"age":{"$gt":25},"city":"Paris"}'
echo ""

# удаление
echo "=== DELETE age = 25 ==="
$BINARY $DB delete '{"age":25}'
echo ""

echo "=== Check Alice deleted ==="
$BINARY $DB find '{"age":25}'
echo ""

echo "=== DELETE city = Paris ==="
$BINARY $DB delete '{"city":"Paris"}'
echo ""

echo "=== Check all remaining ==="
$BINARY $DB find '{}'
echo ""

# проверка персистентности
echo "=== Insert test doc ==="
$BINARY $DB insert '{"test":"persistence","val":123}'
echo ""

echo "=== Find test doc ==="
$BINARY $DB find '{"test":"persistence"}'
echo ""

# проверяем что файл создан
if [ -f "data/${DB}.json" ]; then
    echo "OK: data file exists"
else
    echo "ERROR: data file not found"
fi

echo ""
echo "Tests finished"
rm -f $BINARY

