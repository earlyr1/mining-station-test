CREATE TABLE IF NOT EXISTS processor (
  site_id        text PRIMARY KEY,
  epoch          bigint NOT NULL,
  next_offset    bigint,                -- NULL: ещё ничего не обработано
  now_ts         bigint,                -- «сейчас», секунды epoch
  logic_version  text   NOT NULL,
  config_hash    text   NOT NULL,
  msgs_accepted  bigint NOT NULL DEFAULT 0,  -- счётчики пишутся в чекпоинте вместе с offset:
  msgs_not_newer bigint NOT NULL DEFAULT 0,  -- точные (не сбрасываются при рестарте), их
  msgs_invalid   bigint NOT NULL DEFAULT 0,  -- сравнивают e2e и витрина
  updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stations (
  station_uuid          text PRIMARY KEY,
  occupant_uuid         text,
  occupied_at           bigint,
  last_release_at       bigint,
  last_published_queue  text,           -- канонический JSON строк последней опубликованной очереди; NULL — ещё не публиковалась
  updated_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vehicles (
  unit_uuid          text PRIMARY KEY,
  home_station       text    NOT NULL,
  last_ts            bigint  NOT NULL,
  lat                double precision NOT NULL,
  lon                double precision NOT NULL,
  speed_kmh          double precision NOT NULL,
  phase              text    NOT NULL,  -- to_station | waiting | service | served | to_dump
  target_station     text    NOT NULL,
  radius_station     text,              -- станция, в радиусе 50 м которой машина; ведётся в фазе to_station (§3.5)
  radius_entered_at  bigint,            -- вход в радиус radius_station
  arrived_at         bigint,            -- приезд, от которого считается wait_seconds
  dump_visited       boolean NOT NULL,  -- была в радиусе разгрузки на этом плече
  decided            boolean NOT NULL,  -- рекомендация в этом заезде уже считалась
  predicted_start    bigint,            -- service_start, предсказанный при решении (метрика точности)
  updated_at         timestamptz NOT NULL DEFAULT now()
);
