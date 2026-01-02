# =============================================================================
# Dockerfile - 讓你的程式碼住進「標準貨櫃」
# Dockerfile - Pack your code into a "standard container"
# =============================================================================
# 想像 Docker 就像是「樂高積木的標準規格」：
# Think of Docker as "standardized LEGO bricks":
#   - 不管你在 Windows、Mac 還是 Linux 開發
#     Whether you develop on Windows, Mac, or Linux
#   - 打包後的成品（Image）放到哪裡都能跑
#     The packaged result (Image) runs anywhere
# =============================================================================

# -----------------------------------------------------------------------------
# 第一步：選擇基底環境 (Base Image)
# Step 1: Choose the base environment
# -----------------------------------------------------------------------------
# FROM 指定「底層」是什麼系統
# FROM specifies what the "base layer" system is
#
# python:3.12-slim 表示：
# python:3.12-slim means:
#   - Python 3.12 版 / Python version 3.12
#   - slim = 精簡版 Linux（Image 較小）/ slim = lightweight Linux (smaller image)
#
# 💡 練習 Practice：試試把 3.12 改成 3.11，重新 build 看看
#    Try changing 3.12 to 3.11 and rebuild
# -----------------------------------------------------------------------------
FROM python:3.12-slim

# -----------------------------------------------------------------------------
# 第二步：設定工作目錄
# Step 2: Set the working directory
# -----------------------------------------------------------------------------
# WORKDIR 就像 cd 到某個資料夾
# WORKDIR is like using cd to enter a folder
# 之後的指令都會在這個資料夾執行
# All subsequent commands will run in this directory
# -----------------------------------------------------------------------------
WORKDIR /app

# -----------------------------------------------------------------------------
# 第三步：複製與安裝依賴
# Step 3: Copy and install dependencies
# -----------------------------------------------------------------------------
# 為什麼先複製 requirements.txt，再複製其他檔案？
# Why copy requirements.txt first, then other files?
#
# → Docker 有「快取層」機制
#   Docker has a "cache layer" mechanism
# → 如果 requirements.txt 沒改，就不會重新安裝套件，加快 build 速度
#   If requirements.txt hasn't changed, packages won't be reinstalled (faster build)
# -----------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------------------
# 第四步：複製應用程式碼
# Step 4: Copy application code
# -----------------------------------------------------------------------------
# COPY <本地端> <容器內>
# COPY <local> <container>
# "." 表示目前目錄的所有檔案
# "." means all files in the current directory
# -----------------------------------------------------------------------------
COPY . .

# -----------------------------------------------------------------------------
# 第五步：告訴 Docker 這個容器會用哪個 Port
# Step 5: Tell Docker which port this container uses
# -----------------------------------------------------------------------------
# EXPOSE 只是「標註」，實際開放還是要在 docker run 時指定 -p
# EXPOSE is just a "label", actual port mapping requires -p in docker run
# FastAPI 預設 port 是 8000
# FastAPI default port is 8000
# -----------------------------------------------------------------------------
EXPOSE 8000

# -----------------------------------------------------------------------------
# 第六步：設定容器啟動時執行的指令
# Step 6: Set the command to run when container starts
# -----------------------------------------------------------------------------
# CMD 是容器「啟動時」執行的預設指令
# CMD is the default command executed when container "starts"
#
# 參數說明 Parameter explanation:
#   - uvicorn：ASGI 伺服器 / ASGI server for running FastAPI
#   - main:app：main.py 裡面的 app 物件 / the app object in main.py
#   - --host 0.0.0.0：接受所有來源的連線 / accept connections from all sources
#   - --port 8000：監聽的 port / listening port
# -----------------------------------------------------------------------------
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# =============================================================================
# 🚀 試著跑看看！ Try it out!
# =============================================================================
#
# 1. 建立 Image Build the Image（在這個資料夾執行 run in this folder）：
#    docker build -t my-fastapi-app .
#
# 2. 啟動 Container Start the Container：
#    docker run -p 8000:8000 my-fastapi-app
#
# 3. 打開瀏覽器 Open browser：
#    http://localhost:8000
#    http://localhost:8000/docs
#
# =============================================================================
