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

import io.trino.util.CronExpressionParser.CronExpression;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCronExpressionParser
{
    @Test
    void testValidExpressions()
    {
        assertValid("0 * * * *");
        assertValid("*/15 * * * *");
        assertValid("0 0 * * *");
        assertValid("0 0 * * 1-5");
        assertValid("0 0 1,15 * *");
        assertValid("30 4 1-7 * 1");
        assertValid("0 0 * * 0");
        assertValid("0 0 * * 7");
        assertValid("5,10,15 * * * *");
        assertValid("0 0-5 * * *");
        assertValid("0 */2 * * *");
    }

    @Test
    void testInvalidExpressions()
    {
        assertInvalid("invalid");
        assertInvalid("0 * *");
        assertInvalid("60 * * * *");
        assertInvalid("* 25 * * *");
        assertInvalid("* * 0 * *");
        assertInvalid("* * * 13 *");
        assertInvalid("* * * * 8");
        assertInvalid("");
        assertInvalid("0 * * * * *");
    }

    @Test
    void testNextFireTimeEveryHour()
    {
        CronExpression expr = CronExpressionParser.parse("0 * * * *");
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 1, 10, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(now);
        assertThat(next.getHour()).isEqualTo(11);
        assertThat(next.getMinute()).isEqualTo(0);
        assertThat(next.getDayOfMonth()).isEqualTo(1);
    }

    @Test
    void testNextFireTimeMidnightDaily()
    {
        CronExpression expr = CronExpressionParser.parse("0 0 * * *");
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 1, 10, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(now);
        assertThat(next.getDayOfMonth()).isEqualTo(2);
        assertThat(next.getHour()).isEqualTo(0);
        assertThat(next.getMinute()).isEqualTo(0);
    }

    @Test
    void testNextFireTimeEvery15Minutes()
    {
        CronExpression expr = CronExpressionParser.parse("*/15 * * * *");
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 1, 10, 14, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(now);
        assertThat(next.getMinute()).isEqualTo(15);
        assertThat(next.getHour()).isEqualTo(10);
    }

    @Test
    void testNextFireTimeWeekdaysOnly()
    {
        CronExpression expr = CronExpressionParser.parse("0 0 * * 1-5");
        // 2025-01-04 is a Saturday
        ZonedDateTime saturday = ZonedDateTime.of(2025, 1, 4, 10, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(saturday);
        // Next should be Monday Jan 6
        assertThat(next.getDayOfMonth()).isEqualTo(6);
        assertThat(next.getHour()).isEqualTo(0);
        assertThat(next.getMinute()).isEqualTo(0);
    }

    @Test
    void testNextFireTimeSpecificDays()
    {
        CronExpression expr = CronExpressionParser.parse("0 0 1,15 * *");
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 2, 0, 0, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(now);
        assertThat(next.getDayOfMonth()).isEqualTo(15);
    }

    @Test
    void testMatches()
    {
        CronExpression expr = CronExpressionParser.parse("30 14 * * *");
        ZonedDateTime matching = ZonedDateTime.of(2025, 6, 15, 14, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime notMatching = ZonedDateTime.of(2025, 6, 15, 14, 31, 0, 0, ZoneId.of("UTC"));
        assertThat(expr.matches(matching)).isTrue();
        assertThat(expr.matches(notMatching)).isFalse();
    }

    @Test
    void testSundayNormalization()
    {
        // Both 0 and 7 mean Sunday
        CronExpression expr0 = CronExpressionParser.parse("0 0 * * 0");
        CronExpression expr7 = CronExpressionParser.parse("0 0 * * 7");
        assertThat(expr0.daysOfWeek()).isEqualTo(expr7.daysOfWeek());
    }

    @Test
    void testEveryMinute()
    {
        CronExpression expr = CronExpressionParser.parse("* * * * *");
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 1, 10, 30, 0, 0, ZoneId.of("UTC"));
        ZonedDateTime next = expr.nextFireTime(now);
        assertThat(next.getMinute()).isEqualTo(31);
        assertThat(next.getHour()).isEqualTo(10);
    }

    private static void assertValid(String expression)
    {
        CronExpressionParser.parse(expression);
    }

    private static void assertInvalid(String expression)
    {
        assertThatThrownBy(() -> CronExpressionParser.parse(expression))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
