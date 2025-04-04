#!/bin/bash

echo "⏳ Esperando a que LocalStack esté disponible..."

until curl -s http://localstack:4566/health | grep '"s3": "running"' > /dev/null; do
  sleep 2
done

echo "✅ LocalStack está listo."

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "📦 Creando bucket S3 'datalake' si no existe..."
aws --endpoint-url=http://localstack:4566 s3 mb s3://datalake || echo "⚠️  El bucket ya existe."

echo "✅ Inicialización completada."
