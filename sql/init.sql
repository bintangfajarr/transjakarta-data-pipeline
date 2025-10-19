CREATE DATABASE transjakarta_dwh;

\c transjakarta_dwh;

CREATE TABLE IF NOT EXISTS dummy_transaksi_bus (
    uuid VARCHAR(255) PRIMARY KEY,
    waktu_transaksi TIMESTAMP,
    armada_id_var VARCHAR(50),
    no_body_var VARCHAR(50),
    card_number_var VARCHAR(100),
    card_type_var VARCHAR(50),
    balance_before_int INTEGER,
    fare_int INTEGER,
    balance_after_int INTEGER,
    transcode_txt VARCHAR(50),
    gate_in_boo BOOLEAN,
    p_latitude_flo FLOAT,
    p_longitude_flo FLOAT,
    status_var VARCHAR(10),
    free_service_boo BOOLEAN,
    insert_on_dtm TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dummy_transaksi_halte (
    uuid VARCHAR(255) PRIMARY KEY,
    waktu_transaksi TIMESTAMP,
    shelter_name_var VARCHAR(255),
    terminal_name_var VARCHAR(255),
    card_number_var VARCHAR(100),
    card_type_var VARCHAR(50),
    balance_before_int INTEGER,
    fare_int INTEGER,
    balance_after_int INTEGER,
    transcode_txt VARCHAR(50),
    gate_in_boo BOOLEAN,
    p_latitude_flo FLOAT,
    p_longitude_flo FLOAT,
    status_var VARCHAR(10),
    free_service_boo BOOLEAN,
    insert_on_dtm TIMESTAMP
);

CREATE TABLE IF NOT EXISTS output_by_card_type (
    id SERIAL PRIMARY KEY,
    tanggal DATE,
    card_type VARCHAR(50),
    gate_in_boo BOOLEAN,
    jumlah_pelanggan INTEGER,
    total_amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS output_by_route (
    id SERIAL PRIMARY KEY,
    tanggal DATE,
    route_code VARCHAR(50),
    route_name VARCHAR(255),
    gate_in_boo BOOLEAN,
    jumlah_pelanggan INTEGER,
    total_amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS output_by_tarif (
    id SERIAL PRIMARY KEY,
    tanggal DATE,
    tarif INTEGER,
    gate_in_boo BOOLEAN,
    jumlah_pelanggan INTEGER,
    total_amount BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transaksi_bus_waktu ON dummy_transaksi_bus (waktu_transaksi);

CREATE INDEX idx_transaksi_halte_waktu ON dummy_transaksi_halte (waktu_transaksi);

CREATE INDEX idx_output_card_tanggal ON output_by_card_type (tanggal);

CREATE INDEX idx_output_route_tanggal ON output_by_route (tanggal);

CREATE INDEX idx_output_tarif_tanggal ON output_by_tarif (tanggal);