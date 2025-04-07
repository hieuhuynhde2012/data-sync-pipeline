FROM mysql:latest

# Thiết lập biến môi trường để tránh cảnh báo
ENV MYSQL_ALLOW_EMPTY_PASSWORD=yes

# Sao chép file schema.sql để MySQL tự động khởi tạo
COPY schema.sql /docker-entrypoint-initdb.d/

# Chuyển thư mục làm việc sang /app
WORKDIR /app

# Cài đặt Python bằng cách sử dụng official image của Python
FROM python:3.9-slim AS python_env

# Sao chép file requirements.txt vào container
COPY requirements.txt /app/requirements.txt

# Cài đặt các dependencies từ requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Sao chép file connect_mysql.py vào container
COPY connect_mysql.py /app/

# Chạy container ở chế độ mặc định
