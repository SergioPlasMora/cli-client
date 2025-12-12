# cli-client

# Reemplaza TENANT_ID con el ID real de tu connector (ej: tenant_desktop_xyz)

python cli.py query --tenant tenant_desktop_cfiot58

# Para probar el flujo real con tu único connector conectado:
python cli.py load-test --requests 50 --concurrency 5 --tenants-list tenant_desktop_cfiot58

# Pequeño (~1MB, ~16k filas)
python cli.py query --tenant tenant_desktop_cfiot58 --rows 16000

# Mediano (~6MB, ~100k filas)
python cli.py query --tenant tenant_desktop_cfiot58 --rows 100000

# Tu prueba original (~62MB, 1M filas)
python cli.py query --tenant tenant_desktop_cfiot58 --rows 1000000


# Reiniciar el Connector para cargar el nuevo código
# Luego probar con tu CSV de 10MB:
python cli.py query --tenant tenant_desktop_cfiot58 --dataset dataset_10mb

# CSV de 50MB:
python cli.py query --tenant tenant_desktop_cfiot58 --dataset dataset_50mb

# CSV de 100MB:
python cli.py query --tenant tenant_desktop_cfiot58 --dataset dataset_100mb

# CSV de 10MB con 50 usuarios intentando descargar a la vez (100 descargas):
python cli.py load-test --requests 1 --concurrency 1 --dataset dataset_40mb --tenants-list tenant_desktop_cfiot58

tenant_desktop_cfiot58