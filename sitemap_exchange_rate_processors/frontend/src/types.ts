export type ProductObservation = {
  product_id: string;
  product_name: string | null;
  product_url: string | null;
  price: number | string;
  currency: string;
  observed_at: string;
  emitted_at: string;
  created_at: string;
  kafka_topic: string;
};

export type ExchangeRateObservation = {
  base_currency: string;
  target_currency: string;
  rate: number | string;
  observed_at: string;
  emitted_at: string;
  created_at: string;
  kafka_topic: string;
};

export type SessionLogEntry = {
  timestamp: string;
  level: "info" | "success" | "error" | string;
  message: string;
};

export type SessionData = {
  session_id: string;
  price_topic: string;
  rate_topic: string;
  database_status: "ok" | "error" | string;
  database_error: string | null;
  products_count: number;
  exchange_rates_count: number;
  products: ProductObservation[];
  exchange_rates: ExchangeRateObservation[];
  logs: SessionLogEntry[];
};

export type PipelineRunRequest = {
  price_url: string;
  currency_url: string;
  kafka_servers: string;
  base_currency: string;
  consumer_timeout_seconds: number;
  persist_wait_seconds: number;
};

export type PipelineRunResponse = {
  status: string;
  message: string;
  session_id: string;
  price_topic: string;
  rate_topic: string;
  product_records_published: number;
  currency_records_published: number;
};

export type ApiConfig = {
  default_currency_url: string;
  default_kafka_servers: string;
  default_price_topic: string;
  default_rate_topic: string;
  default_base_currency: string;
  database_url_configured: boolean;
  pipeline_endpoint: string;
  session_pipeline_endpoint: string;
  session_data_endpoint: string;
};
