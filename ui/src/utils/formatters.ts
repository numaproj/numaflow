export const formatNumber = (value?: number) =>
  (value ?? 0).toLocaleString("en-US");

export const formatDecimal = (value: number, precision = 2) => {
  const multiplier = Math.pow(10, precision);
  return (
    Math.round((value + Number.EPSILON) * multiplier) / multiplier
  ).toFixed(precision);
};

const formatCompactDecimal = (value: number) => {
  const formatted = formatDecimal(value, 1);
  return formatted.endsWith(".0") ? formatted.slice(0, -2) : formatted;
};

export const formatCompactCount = (value?: number) => {
  const count = value ?? 0;
  const absoluteCount = Math.abs(count);
  if (absoluteCount >= 1_000_000) {
    return `${formatCompactDecimal(count / 1_000_000)}M`;
  }
  if (absoluteCount >= 1_000) {
    return `${formatCompactDecimal(count / 1_000)}k`;
  }
  return formatNumber(count);
};

export const formatBytes = (value?: number) => {
  const bytes = value ?? 0;
  if (bytes === 0) {
    return "0 B";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1
  );
  const scaled = bytes / Math.pow(1024, index);
  return `${index === 0 ? formatNumber(scaled) : formatDecimal(scaled)} ${
    units[index]
  }`;
};
