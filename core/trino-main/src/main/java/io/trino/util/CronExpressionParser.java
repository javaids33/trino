/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.util;

import com.google.common.collect.ImmutableSortedSet;

import java.time.ZonedDateTime;
import java.util.SortedSet;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Minimal 5-field Unix cron expression parser.
 * Format: minute hour day-of-month month day-of-week
 * <p>
 * Supports: *, specific values, ranges (1-5), steps (* /5), lists (1,3,5)
 */
public final class CronExpressionParser
{
    private CronExpressionParser() {}

    public static CronExpression parse(String expression)
    {
        requireNonNull(expression, "expression is null");
        String trimmed = expression.trim();
        String[] fields = trimmed.split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException(format("Cron expression must have exactly 5 fields, found %d: '%s'", fields.length, expression));
        }

        SortedSet<Integer> minutes = parseField(fields[0], 0, 59, "minute");
        SortedSet<Integer> hours = parseField(fields[1], 0, 23, "hour");
        SortedSet<Integer> daysOfMonth = parseField(fields[2], 1, 31, "day-of-month");
        SortedSet<Integer> months = parseField(fields[3], 1, 12, "month");
        SortedSet<Integer> daysOfWeek = parseDayOfWeekField(fields[4]);

        return new CronExpression(trimmed, minutes, hours, daysOfMonth, months, daysOfWeek);
    }

    public static void validate(String expression)
    {
        parse(expression);
    }

    private static SortedSet<Integer> parseField(String field, int min, int max, String fieldName)
    {
        ImmutableSortedSet.Builder<Integer> values = ImmutableSortedSet.naturalOrder();

        for (String part : field.split(",")) {
            if (part.contains("/")) {
                parseStepExpression(part, min, max, fieldName, values);
            }
            else if (part.contains("-")) {
                parseRangeExpression(part, min, max, fieldName, values);
            }
            else if (part.equals("*")) {
                for (int i = min; i <= max; i++) {
                    values.add(i);
                }
            }
            else {
                int value = parseIntValue(part, fieldName);
                validateRange(value, min, max, fieldName);
                values.add(value);
            }
        }

        SortedSet<Integer> result = values.build();
        if (result.isEmpty()) {
            throw new IllegalArgumentException(format("No values for %s field: '%s'", fieldName, field));
        }
        return result;
    }

    private static void parseStepExpression(String part, int min, int max, String fieldName, ImmutableSortedSet.Builder<Integer> values)
    {
        String[] stepParts = part.split("/", 2);
        int step = parseIntValue(stepParts[1], fieldName + " step");
        if (step <= 0) {
            throw new IllegalArgumentException(format("Step value must be positive for %s: '%s'", fieldName, part));
        }

        int rangeStart;
        int rangeEnd = max;
        if (stepParts[0].equals("*")) {
            rangeStart = min;
        }
        else if (stepParts[0].contains("-")) {
            String[] rangeParts = stepParts[0].split("-", 2);
            rangeStart = parseIntValue(rangeParts[0], fieldName);
            rangeEnd = parseIntValue(rangeParts[1], fieldName);
            validateRange(rangeStart, min, max, fieldName);
            validateRange(rangeEnd, min, max, fieldName);
        }
        else {
            rangeStart = parseIntValue(stepParts[0], fieldName);
            validateRange(rangeStart, min, max, fieldName);
        }

        for (int i = rangeStart; i <= rangeEnd; i += step) {
            values.add(i);
        }
    }

    private static void parseRangeExpression(String part, int min, int max, String fieldName, ImmutableSortedSet.Builder<Integer> values)
    {
        String[] rangeParts = part.split("-", 2);
        int rangeStart = parseIntValue(rangeParts[0], fieldName);
        int rangeEnd = parseIntValue(rangeParts[1], fieldName);
        validateRange(rangeStart, min, max, fieldName);
        validateRange(rangeEnd, min, max, fieldName);
        if (rangeStart > rangeEnd) {
            throw new IllegalArgumentException(format("Invalid range for %s: %d-%d", fieldName, rangeStart, rangeEnd));
        }
        for (int i = rangeStart; i <= rangeEnd; i++) {
            values.add(i);
        }
    }

    private static SortedSet<Integer> parseDayOfWeekField(String field)
    {
        // Day of week: 0-7, where both 0 and 7 represent Sunday
        SortedSet<Integer> parsed = parseField(field, 0, 7, "day-of-week");
        // Normalize 7 (Sunday) to 0
        ImmutableSortedSet.Builder<Integer> normalized = ImmutableSortedSet.naturalOrder();
        for (int value : parsed) {
            normalized.add(value == 7 ? 0 : value);
        }
        return normalized.build();
    }

    private static int parseIntValue(String value, String fieldName)
    {
        try {
            return Integer.parseInt(value.trim());
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("Invalid integer value '%s' in %s field", value, fieldName));
        }
    }

    private static void validateRange(int value, int min, int max, String fieldName)
    {
        if (value < min || value > max) {
            throw new IllegalArgumentException(format("Value %d out of range [%d-%d] for %s", value, min, max, fieldName));
        }
    }

    public record CronExpression(
            String raw,
            SortedSet<Integer> minutes,
            SortedSet<Integer> hours,
            SortedSet<Integer> daysOfMonth,
            SortedSet<Integer> months,
            SortedSet<Integer> daysOfWeek)
    {
        public CronExpression
        {
            requireNonNull(raw, "raw is null");
            minutes = ImmutableSortedSet.copyOfSorted(minutes);
            hours = ImmutableSortedSet.copyOfSorted(hours);
            daysOfMonth = ImmutableSortedSet.copyOfSorted(daysOfMonth);
            months = ImmutableSortedSet.copyOfSorted(months);
            daysOfWeek = ImmutableSortedSet.copyOfSorted(daysOfWeek);
        }

        public boolean matches(ZonedDateTime time)
        {
            int dayOfWeekValue = time.getDayOfWeek().getValue() % 7; // Monday=1..Sunday=0
            return minutes.contains(time.getMinute())
                    && hours.contains(time.getHour())
                    && daysOfMonth.contains(time.getDayOfMonth())
                    && months.contains(time.getMonthValue())
                    && daysOfWeek.contains(dayOfWeekValue);
        }

        public ZonedDateTime nextFireTime(ZonedDateTime after)
        {
            ZonedDateTime candidate = after.plusMinutes(1)
                    .withSecond(0)
                    .withNano(0);

            // Search forward up to 4 years to find next matching time
            ZonedDateTime limit = after.plusYears(4);
            while (candidate.isBefore(limit)) {
                if (!months.contains(candidate.getMonthValue())) {
                    candidate = advanceToNextMonth(candidate);
                    continue;
                }
                if (!daysOfMonth.contains(candidate.getDayOfMonth()) || !matchesDayOfWeek(candidate)) {
                    candidate = candidate.plusDays(1).withHour(0).withMinute(0);
                    continue;
                }
                if (!hours.contains(candidate.getHour())) {
                    candidate = candidate.plusHours(1).withMinute(0);
                    continue;
                }
                if (!minutes.contains(candidate.getMinute())) {
                    candidate = candidate.plusMinutes(1);
                    continue;
                }
                return candidate;
            }
            throw new IllegalStateException(format("Unable to find next fire time for cron expression '%s' within 4 years of %s", raw, after));
        }

        private boolean matchesDayOfWeek(ZonedDateTime time)
        {
            int dayOfWeekValue = time.getDayOfWeek().getValue() % 7;
            return daysOfWeek.contains(dayOfWeekValue);
        }

        private ZonedDateTime advanceToNextMonth(ZonedDateTime candidate)
        {
            return candidate.plusMonths(1).withDayOfMonth(1).withHour(0).withMinute(0);
        }
    }
}
