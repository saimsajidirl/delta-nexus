import { FormEvent, useEffect, useMemo, useState } from "react";
import { fetchApiConfig, fetchSessionData, runSessionPipeline } from "./api";
import type {
  ExchangeRateObservation,
  ProductObservation,
  SessionData,
  SessionLogEntry
} from "./types";

const DEFAULT_PRICE_URL =
  "https://feeds.datafeedwatch.com/25986/cbdd197d9c7747c13f08f840f8bc76eb350292fc.xml";
const DEFAULT_CURRENCY_URL = "https://www.floatrates.com/";
const SESSION_STORAGE_KEY = "delta-nexus-session-id";

function createSessionId(): string {
  const randomPart =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID().replace(/-/g, "").slice(0, 18)
      : Math.random().toString(36).slice(2, 20);
  return `session_${randomPart}`;
}

function getStoredSessionId(): string {
  const existing = localStorage.getItem(SESSION_STORAGE_KEY);
  if (existing) {
    return existing;
  }

  const sessionId = createSessionId();
  localStorage.setItem(SESSION_STORAGE_KEY, sessionId);
  return sessionId;
}

function formatDate(value: string): string {
  if (!value) {
    return "-";
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(value));
}

function formatNumber(value: number | string, digits = 4): string {
  const numericValue = Number(value);
  if (Number.isNaN(numericValue)) {
    return String(value);
  }
  return new Intl.NumberFormat(undefined, {
    maximumFractionDigits: digits
  }).format(numericValue);
}

function money(value: number | string | undefined): string {
  if (value === undefined) {
    return "$0";
  }
  return `$${formatNumber(value, 0)}`;
}

function percent(value: number, max: number): number {
  if (max <= 0) {
    return 0;
  }
  return Math.max(6, Math.min(100, Math.round((value / max) * 100)));
}

export default function App() {
  const [sessionId, setSessionId] = useState(getStoredSessionId);
  const [priceUrl, setPriceUrl] = useState(DEFAULT_PRICE_URL);
  const [currencyUrl, setCurrencyUrl] = useState(DEFAULT_CURRENCY_URL);
  const [baseCurrency, setBaseCurrency] = useState("USD");
  const [kafkaServers, setKafkaServers] = useState("localhost:9092");
  const [sessionData, setSessionData] = useState<SessionData | null>(null);
  const [hasStartedRun, setHasStartedRun] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [statusMessage, setStatusMessage] = useState("Ready to scrape");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const products = sessionData?.products ?? [];
  const exchangeRates = sessionData?.exchange_rates ?? [];
  const logs = sessionData?.logs ?? [];
  const latestRates = useMemo(() => exchangeRates.slice(0, 4), [exchangeRates]);
  const latestProducts = useMemo(() => products.slice(0, 4), [products]);
  const maxPrice = Math.max(...latestProducts.map((product) => Number(product.price) || 0), 1);
  const databaseError = sessionData?.database_status === "error" ? sessionData.database_error : null;
  const visibleError = errorMessage ?? databaseError;
  const showRuntimeError = Boolean(visibleError && hasStartedRun);
  const statusLabel = showRuntimeError ? "Attention" : isRunning ? "Running" : "Online";

  useEffect(() => {
    fetchApiConfig()
      .then((config) => {
        setCurrencyUrl(config.default_currency_url);
        setBaseCurrency(config.default_base_currency);
        setKafkaServers(config.default_kafka_servers);
      })
      .catch(() => {
        setStatusMessage("Ready to scrape");
      });
  }, []);

  async function refreshSessionData(options: { showErrors?: boolean } = {}) {
    try {
      const data = await fetchSessionData(sessionId);
      setSessionData(data);
      setErrorMessage(null);
    } catch (error) {
      if (options.showErrors || hasStartedRun || isRunning) {
        setErrorMessage(error instanceof Error ? error.message : "Unable to load session data");
      }
    }
  }

  useEffect(() => {
    refreshSessionData({ showErrors: false });
    const intervalId = window.setInterval(
      () => refreshSessionData({ showErrors: false }),
      2000
    );
    return () => window.clearInterval(intervalId);
  }, [sessionId, hasStartedRun, isRunning]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setHasStartedRun(true);
    setIsRunning(true);
    setErrorMessage(null);
    setStatusMessage("Pipeline running");

    try {
      const response = await runSessionPipeline(sessionId, {
        price_url: priceUrl,
        currency_url: currencyUrl,
        kafka_servers: kafkaServers,
        base_currency: baseCurrency.toUpperCase(),
        consumer_timeout_seconds: 60,
        persist_wait_seconds: 20
      });
      setStatusMessage(
        `${response.product_records_published} products and ${response.currency_records_published} FX rates published`
      );
      await refreshSessionData({ showErrors: true });
    } catch (error) {
      setStatusMessage("Pipeline failed");
      setErrorMessage(error instanceof Error ? error.message : "Pipeline run failed");
    } finally {
      setIsRunning(false);
    }
  }

  function resetSession() {
    const nextSessionId = createSessionId();
    localStorage.setItem(SESSION_STORAGE_KEY, nextSessionId);
    setSessionId(nextSessionId);
    setSessionData(null);
    setHasStartedRun(false);
    setStatusMessage("Ready to scrape");
    setErrorMessage(null);
  }

  return (
    <main className="dashboard-shell">
      <Topbar statusLabel={statusLabel} />

      <div className="dashboard-body">
        <section className="dashboard-canvas">
          <section className="upper-grid">
            <article className="panel chart-panel">
              <PanelHeader title="Delta Nexus Pipeline" subtitle={statusMessage} />
              <RunForm
                priceUrl={priceUrl}
                currencyUrl={currencyUrl}
                baseCurrency={baseCurrency}
                kafkaServers={kafkaServers}
                isRunning={isRunning}
                onSubmit={handleSubmit}
                onPriceUrlChange={setPriceUrl}
                onCurrencyUrlChange={setCurrencyUrl}
                onBaseCurrencyChange={setBaseCurrency}
                onKafkaServersChange={setKafkaServers}
              />
              <AreaChart products={products} />
            </article>

            <article className="panel radial-panel">
              <PanelHeader title="Session Health" subtitle={showRuntimeError ? "Needs attention" : "Live"} />
              <SessionDonut
                productsCount={sessionData?.products_count ?? 0}
                ratesCount={sessionData?.exchange_rates_count ?? 0}
              />
              <button className="small-dark-button" type="button" onClick={resetSession}>
                New Session
              </button>
            </article>
          </section>

          {showRuntimeError && (
            <section className="alert-strip">
              <strong>Postgres or pipeline error</strong>
              <span>{visibleError}</span>
            </section>
          )}

          <section className="lower-grid">
            <article className="panel metrics-panel">
              <PanelHeader title="Scrape Totals" subtitle={sessionData?.price_topic ?? "Session topic pending"} />
              <div className="money-grid">
                <MetricTile label="Products" value={sessionData?.products_count ?? 0} />
                <MetricTile label="FX Rates" value={sessionData?.exchange_rates_count ?? 0} />
                <MetricTile label="Top Price" value={money(latestProducts[0]?.price)} />
                <MetricTile label="Polling" value="2s" />
              </div>
            </article>

            <article className="panel bars-panel">
              <PanelHeader title="Recent Prices" subtitle="Persisted rows" />
              <BarList products={latestProducts} maxPrice={maxPrice} />
            </article>
          </section>

          <section className="bottom-grid">
            <article className="panel logs-panel">
              <PanelHeader title="Activity Board" subtitle={`${logs.length} log events`} />
              <LogList logs={logs} />
            </article>

            <article className="panel rates-panel">
              <PanelHeader title="Exchange Snapshot" subtitle={sessionData?.rate_topic ?? "Waiting for topic"} />
              <RateList rates={latestRates} />
            </article>
          </section>
        </section>
      </div>
    </main>
  );
}

function Topbar({
  statusLabel
}: {
  statusLabel: string;
}) {
  return (
    <header className="topbar">
      <div className="brand-name">DELTA NEXUS</div>
      <div className="top-user">
        <div>
          <strong>{statusLabel}</strong>
          <small>Session Console</small>
        </div>
      </div>
    </header>
  );
}

function PanelHeader({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <div className="panel-header">
      <div>
        <h2>{title}</h2>
        <p>{subtitle}</p>
      </div>
    </div>
  );
}

function RunForm({
  priceUrl,
  currencyUrl,
  baseCurrency,
  kafkaServers,
  isRunning,
  onSubmit,
  onPriceUrlChange,
  onCurrencyUrlChange,
  onBaseCurrencyChange,
  onKafkaServersChange
}: {
  priceUrl: string;
  currencyUrl: string;
  baseCurrency: string;
  kafkaServers: string;
  isRunning: boolean;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onPriceUrlChange: (value: string) => void;
  onCurrencyUrlChange: (value: string) => void;
  onBaseCurrencyChange: (value: string) => void;
  onKafkaServersChange: (value: string) => void;
}) {
  return (
    <form className="run-form" onSubmit={onSubmit}>
      <label>
        Product feed URL
        <input value={priceUrl} onChange={(event) => onPriceUrlChange(event.target.value)} />
      </label>
      <details>
        <summary>Connection settings</summary>
        <div className="settings-grid">
          <input value={currencyUrl} onChange={(event) => onCurrencyUrlChange(event.target.value)} />
          <input
            value={baseCurrency}
            onChange={(event) => onBaseCurrencyChange(event.target.value.slice(0, 3))}
          />
          <input value={kafkaServers} onChange={(event) => onKafkaServersChange(event.target.value)} />
        </div>
      </details>
      <button className="primary-action" disabled={isRunning}>
        {isRunning ? "RUNNING" : "START SCRAPE"}
      </button>
    </form>
  );
}

function AreaChart({ products }: { products: ProductObservation[] }) {
  const values = products.slice(0, 24).map((product) => Number(product.price) || 0);
  const fallback = [4, 2, 2, 4, 7, 8, 6, 4, 3, 3, 4, 7, 6, 4];
  const chartValues = values.length > 2 ? values : fallback;
  const max = Math.max(...chartValues, 1);
  const points = chartValues.map((value, index) => {
    const x = 10 + (index / Math.max(chartValues.length - 1, 1)) * 280;
    const y = 96 - (value / max) * 70;
    return `${x},${y}`;
  });
  const area = `M ${points[0]} L ${points.join(" L ")} L 290,108 L 10,108 Z`;

  return (
    <div className="area-chart">
      <svg viewBox="0 0 300 122" role="img" aria-label="recent product prices">
        <defs>
          <linearGradient id="chartGradient" x1="0" x2="1" y1="0" y2="1">
            <stop offset="0%" stopColor="#4f4960" />
            <stop offset="100%" stopColor="#2f2a3d" />
          </linearGradient>
        </defs>
        <line x1="46" y1="8" x2="46" y2="110" />
        <line x1="96" y1="8" x2="96" y2="110" />
        <line x1="146" y1="8" x2="146" y2="110" />
        <line x1="196" y1="8" x2="196" y2="110" />
        <line x1="246" y1="8" x2="246" y2="110" />
        <path d={area} />
      </svg>
      <div className="chart-months">
        <span>FEED</span>
        <span>KAFKA</span>
        <span>POSTGRES</span>
        <span>UI</span>
      </div>
    </div>
  );
}

function SessionDonut({
  productsCount,
  ratesCount
}: {
  productsCount: number;
  ratesCount: number;
}) {
  const total = Math.max(productsCount + ratesCount, 1);
  const productDegrees = Math.round((productsCount / total) * 360);
  return (
    <div className="donut-wrap">
      <div
        className="donut"
        style={{
          background: `conic-gradient(#403b4d 0deg ${productDegrees}deg, #bfc0c4 ${productDegrees}deg 360deg)`
        }}
      >
        <span>{productsCount + ratesCount}</span>
      </div>
      <p>Total persisted events</p>
    </div>
  );
}

function MetricTile({ label, value }: { label: string; value: number | string }) {
  return (
    <article className="metric-tile">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function BarList({ products, maxPrice }: { products: ProductObservation[]; maxPrice: number }) {
  if (!products.length) {
    return <EmptyState message="No persisted products yet" />;
  }

  return (
    <div className="bar-list">
      {products.map((product) => (
        <article key={`${product.product_id}-${product.created_at}`}>
          <span>{product.product_name ?? product.product_id}</span>
          <div>
            <i style={{ width: `${percent(Number(product.price) || 0, maxPrice)}%` }} />
          </div>
        </article>
      ))}
    </div>
  );
}

function LogList({ logs }: { logs: SessionLogEntry[] }) {
  if (!logs.length) {
    return <EmptyState message="Logs will appear after a scrape starts" />;
  }

  return (
    <div className="log-list">
      {logs
        .slice()
        .reverse()
        .slice(0, 7)
        .map((log, index) => (
          <article className={log.level} key={`${log.timestamp}-${index}`}>
            <time>{formatDate(log.timestamp)}</time>
            <span>{log.level}</span>
            <p>{log.message}</p>
          </article>
        ))}
    </div>
  );
}

function RateList({ rates }: { rates: ExchangeRateObservation[] }) {
  if (!rates.length) {
    return <EmptyState message="Exchange rates will appear here" />;
  }

  return (
    <div className="rate-list">
      {rates.map((rate) => (
        <article key={`${rate.base_currency}-${rate.target_currency}-${rate.created_at}`}>
          <span>
            {rate.base_currency} / {rate.target_currency}
          </span>
          <strong>{formatNumber(rate.rate, 6)}</strong>
        </article>
      ))}
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return <div className="empty-state">{message}</div>;
}
