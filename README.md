```markdown
# Lab 2: Spark Programming - K-Means & Parallelism

## Setup

Before running any tests or Spark jobs, start the cluster using:

```bash
./start.sh
```

This will initialize the Spark environment and required dependencies.

---

## File Structure

```
.
├── datasets/
├── env.sh
├── kmeans_ticket_probability.py     # Part 3a
├── lincoln_center_parking.py        # Part 3b
├── nba_zones_spark.py
├── ticket_probability.py
├── violation_by_month.py
├── test_1.sh                         # Part 1 - Parallelism experiment
├── test_1_t.sh
├── test_2.sh                         # Part 2
├── start.sh
├── stop.sh
└── README.md
```

---

## Part 1: Parallelism Testing

This part tests how parallelism levels affect Spark execution.

Run the following command using a parallelism number between 2 and 5:

```bash
./test_1.sh <parallelism_number>
```

Example:

```bash
./test_1.sh 4
```

This will set `spark.default.parallelism=4` in the job configuration.

---

## Part 2: Reimplementation of Project 1, Q2

To run Part 2, execute:

```bash
./test_2.sh
```

---

## Part 3a: K-Means Ticket Probability

To run the Spark job that clusters based on ticket probability across the city, use:

```bash
/usr/local/spark/bin/spark-submit \
  --master spark://10.128.0.6:7077 \
  kmeans_ticket_probability.py
```

---

## Part 3b: Lincoln Center Parking Zones

To run the zone detection near Lincoln Center:

```bash
/usr/local/spark/bin/spark-submit \
  --master spark://10.128.0.6:7077 \
  lincoln_center_parking.py
```

---

## Teardown

When you're finished, stop the environment using:

```bash
./stop.sh
```
```


