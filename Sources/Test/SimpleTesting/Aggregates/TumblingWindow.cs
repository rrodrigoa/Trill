using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.StreamProcessing;
using Microsoft.StreamProcessing.Aggregates;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting.Aggregates
{
    public static class MyExtensions
    {
        public static IAggregate<TSource, AverageState, string> MyAverage<TKey, TSource>(
        this Window<TKey, TSource> window, Expression<Func<TSource, string>> selector)
        {
            var aggregate = new MyAverageAggregate();
            return aggregate.Wrap(selector);
        }
    }

    [TestClass]
    public class TumblingWindowAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        public static DateTime MidnightUtc(long eventTime)
        {
            DateTime time = new DateTime(eventTime);
            var midnight = new DateTime(time.Year, time.Month, time.Day, 0, 0, 0);
            return midnight;
        }

        public static long AlterEventLifetimeFromMidnight(long eventTime, long windowSize)
        {
            DateTime time = new DateTime(eventTime);

            DateTime mid = new DateTime(eventTime).Date;

            TimeSpan windowSizeSeconds = new TimeSpan(windowSize);
            long win = (long)((time - mid).TotalSeconds / windowSizeSeconds.TotalSeconds);
            TimeSpan diff = TimeSpan.FromSeconds((long)(win * windowSizeSeconds.TotalSeconds));
            DateTime windowStart = mid + diff;
            return windowStart.Ticks;
        }

        [TestMethod, TestCategory("Gated")]
        public void TestTumblingWindowAggregate()
        { 
            /*
             * Time:    0 1 2 3 4 5
             * Input: a |
             *        b   |
             *        c     |
             *        d       |
             *        e         |
             * Output:      a,b at time 2; c, d at time 4; e at time 6
             */
            long windowSize = TimeSpan.FromMinutes(2).Ticks;

            DateTime now = DateTime.Now.Date;
            DateTime nowP1 = now.AddMinutes(1);
            DateTime nowP2 = now.AddMinutes(2);
            DateTime nowP3 = now.AddMinutes(3);
            DateTime nowP4 = now.AddMinutes(4);

            IStreamable<Empty, string> input = new StreamEvent<string>[]
            {
                StreamEvent.CreatePoint(now.Ticks, "a"),
                StreamEvent.CreatePoint(nowP1.Ticks, "b"),
                StreamEvent.CreatePoint(nowP2.Ticks, "c"),
                StreamEvent.CreatePoint(nowP3.Ticks, "d"),
                StreamEvent.CreatePoint(nowP4.Ticks, "e"),
                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            }.ToStreamable()
                .AlterEventLifetime(e => AlterEventLifetimeFromMidnight(e, windowSize), windowSize)
                .ClipEventDuration(1)
                .ShiftEventLifetime(e => windowSize);

            // var output = input.Aggregate(a => a.Count());
            var output = input.Aggregate(a => a.MyAverage(v => v));


            var correct = new[]
            {
                StreamEvent.CreatePoint<string>(now.Ticks + windowSize, "acc: a acc: b"),
                StreamEvent.CreatePoint<string>(now.Ticks + windowSize + windowSize, "acc: c acc: d"),
                StreamEvent.CreatePoint<string>(now.Ticks + windowSize + windowSize + windowSize, "acc: e"),

                StreamEvent.CreatePunctuation<string>(StreamEvent.InfinitySyncTime)
            };

            Assert.IsTrue(output.IsEquivalentTo2(correct));
        }


    }

    public struct AverageState
    {
        public string values;
    }
    public class MyAverageAggregate : IAggregate<string, AverageState, string>
    {
        public Expression<Func<AverageState>> InitialState()
        {
            return () => new AverageState();
        }
        public Expression<Func<AverageState, long, string, AverageState>> Accumulate()
        {
            return (oldState, timestamp, input) => new AverageState
            {
                values = oldState.values + " acc: " + input
            };
        }
        public Expression<Func<AverageState, long, string, AverageState>> Deaccumulate()
        {
            return (oldState, timestamp, input) => new AverageState
            {
                values = oldState.values + " deacc: " + input
            };
        }
        public Expression<Func<AverageState, AverageState, AverageState>> Difference()
        {
            return (left, right) => new AverageState
            {
                values = $" left: {left} right: {right}"
            };
        }
        public Expression<Func<AverageState, string>> ComputeResult()
        {
            return state => state.values;
        }
    }
}
