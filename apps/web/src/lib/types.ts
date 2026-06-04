export type Metric = {
  metric_id: string;
  display_name: string;
  source: string;
  dataset: string;
  unit: string;
  frequency: string;
};

export type Observation = {
  metric_id: string;
  geo_id: string;
  geo_level: string;
  period: string;
  value: number;
  unit: string;
  source: string;
  dataset: string;
};

export type LatestObservationCollection = {
  metric_id: string;
  geo_level: string;
  period: string;
  count: number;
  observations: Observation[];
};
