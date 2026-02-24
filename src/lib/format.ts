import { format, parseISO } from "date-fns";

export function formatIsoDate(input: string): string {
  return format(parseISO(input), "yyyy-MM-dd HH:mm");
}

export function formatNumber(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}
