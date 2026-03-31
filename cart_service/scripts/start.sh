#!/bin/bash

# 安装依赖
echo "Installing dependencies..."
pip install -r requirements.txt

# 启动服务
echo "Starting certificate service..."
export PORT=58000
export PORT=58000
python -m app.server
