# profit-ecommerce

## Despliegue

Dos formas de correr el sincronizador:

- **Docker** (ver `Dockerfile` / `docker-compose.yml`).
- **Servicio de Windows nativo**: se compila `syncer.exe` y se registra como
  servicio con [nssm](https://nssm.cc/) mediante
  `scripts\install-windows-service.ps1`. Esta es la forma en la que corre
  actualmente el servicio `ProfitSyncer` en el servidor.

