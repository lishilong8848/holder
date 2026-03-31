# 使用 Python 3.10 轻量基础镜像
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 安装 Chromium 和相关依赖 (用于 Selenium)
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libasound2 \
    fonts-ipafont-gothic fonts-wqy-zenhei \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目所有文件
COPY . .

# 设置环境变量，确保 Selenium 能找到容器内的浏览器
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

# 暴露端口 (默认 58000)
EXPOSE 58000

# 启动命令
# 启动命令
CMD ["python", "-m", "app.server"]
