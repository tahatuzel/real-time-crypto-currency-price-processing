# Temiz bir Python 3.10 imajı kullan
FROM python:3.10-slim

# Çalışma dizini belirle
WORKDIR /app

# Önce sadece bağımlılıkları kopyala ve yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Sonra uygulama kodunu kopyala
COPY producer.py .

# Konteyner başladığında bu komutu çalıştır
CMD ["python", "producer.py"]