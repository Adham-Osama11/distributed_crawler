@echo off
echo Running component tests for distributed web crawler

echo.
echo === Testing Master Node ===
python src\tests\test_master_node.py

echo.
echo === Testing Crawler Node ===
python src\tests\test_crawler_node.py

echo.
echo === Testing indexer Node ===
python src\tests\test_indexer_node.py
echo.
echo All tests completed.
pause

