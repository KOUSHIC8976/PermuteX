# processing/init_db.py
import psycopg2 

def init_postgres():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost",
        database="permutex_db",
        user="permutex_user",
        password="permutex_password"
    )
    cur = conn.cursor()

    print("Creating Gold Layer Table: satellite_telemetry_gold...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS satellite_telemetry_gold (
            id SERIAL PRIMARY KEY,
            satellite_id VARCHAR(50),
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            max_temperature DOUBLE PRECISION,
            avg_cpu DOUBLE PRECISION
        );
        
        -- Create indexes for extremely fast dashboard querying
        CREATE INDEX IF NOT EXISTS idx_satellite_time 
        ON satellite_telemetry_gold (satellite_id, window_end DESC);
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    print("Database successfully initialized!")

if __name__ == '__main__':
    init_postgres()