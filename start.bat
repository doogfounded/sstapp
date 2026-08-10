@echo off
echo Starting SSTApp Stack...
start "Server" cmd /k "cd server && go run main.go"
start "Visualizer" cmd /k "cd visualizer && go run main.go"
start "Traffic" cmd /k "cd traffic && go run main.go normal 3 10"
