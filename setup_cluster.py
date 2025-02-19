# setup_cluster.py
from dask.distributed import LocalCluster, Client
import sys
import os
import logging
import dask

dask.config.set({"distributed.comm.tcp.high-water": "1GiB"})
dask.config.set({"distributed.worker.memory.target": 0.85})
dask.config.set({"distributed.worker.memory.spill": 0.90})
dask.config.set({"distributed.worker.memory.pause": 0.95})

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("DaskCluster")

def setup_master_scheduler():
    """Inicia o nó master (scheduler + worker)."""
    cluster = LocalCluster(
        scheduler_port=8786,
        dashboard_address=':8787',
        processes=True,
        n_workers=2,
        threads_per_worker=12,
        memory_limit="24GB",
        local_directory="/tmp/dask-master"
    )
    client = Client(cluster)
    
    scheduler_ip = cluster.scheduler_address
    logger.info(f"Master+Scheduler iniciado em {scheduler_ip}")
    logger.info(f"Dashboard disponível em http://localhost:8787/status")
    logger.info("Recursos alocados: 8 threads, 24GB RAM")
    
    return cluster, client

def setup_worker(scheduler_address, n_cores=None, memory_limit=None):
    """Inicia um worker conectado ao scheduler."""
    if n_cores is None:
        n_cores = 12
    
    if memory_limit is None:
        memory_limit = "26GB"
    
    temp_dir = f"/tmp/dask-worker-{os.getpid()}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        client = Client(
            scheduler_address,
            local_directory=temp_dir,
            memory_limit=memory_limit,
            n_workers=1,
            threads_per_worker=n_cores
        )
        logger.info(f"Worker conectado ao scheduler {scheduler_address}")
        logger.info(f"Recursos: {n_cores} threads, {memory_limit} RAM")
        logger.info(f"Diretório temporário: {temp_dir}")
        return client
    except Exception as e:
        logger.error(f"Erro ao conectar ao scheduler: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python setup_cluster.py [master|worker] [opções]")
        print("  Para iniciar master+scheduler: python setup_cluster.py master")
        print("  Para iniciar worker: python setup_cluster.py worker tcp://ip-do-master:8786 [n_cores] [memory_limit]")
        sys.exit(1)
        
    mode = sys.argv[1].lower()
    
    if mode == "master":
        cluster, client = setup_master_scheduler()
        try:
            input("Master+Scheduler em execução. Pressione Enter para encerrar...")
        except KeyboardInterrupt:
            pass
        finally:
            client.close()
            cluster.close()
        
    elif mode == "worker":
        if len(sys.argv) < 3:
            print("Endereço do scheduler necessário para o worker")
            sys.exit(1)
            
        scheduler_address = sys.argv[2]
        n_cores = int(sys.argv[3]) if len(sys.argv) > 3 else None
        memory_limit = sys.argv[4] if len(sys.argv) > 4 else None
        
        client = setup_worker(scheduler_address, n_cores, memory_limit)
        if client:
            try:
                input("Worker em execução. Pressione Enter para encerrar...")
            except KeyboardInterrupt:
                pass
            finally:
                client.close()
        else:
            sys.exit(1)
    else:
        print(f"Modo desconhecido: {mode}")
        sys.exit(1)
