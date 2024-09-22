# Employee Data Management System - Data Engineering Solution
Here’s a detailed README for your Employee Data Management System project. This README will give potential users or reviewers a clear understanding of your project’s purpose, setup, and usage.


## 1. Project Overview

The **Employee Data Management System** is designed to efficiently manage, process, and analyze employee data across an organization. The system ingests employee-related data from multiple sources in both batch and real-time formats and uses AWS services to store, process, and monitor data.

The goal of the system is to:
- Manage employee data.
- Track leave quotas and identify potential misuse.
- Monitor communication for policy violations.

This system uses AWS for cloud-based storage, processing, and reporting. Data is ingested, transformed, and stored daily, with features to track employee activity and generate reports.


## 2. Key Features
- **Data Ingestion:** Automated ingestion of employee and leave data from CSV and JSON files.
- **Data Processing:** Processes incremental updates, handles duplicate records, and marks active/inactive employees.
- **Reporting:** Generates daily and monthly reports to track employee leave history and potential abuse.
- **Communication Monitoring:** Real-time monitoring of employee communication streams using Kafka, with strike-based salary deductions for flagged messages.
- **Scalability:** Built using AWS services for scalable, secure, and fault-tolerant infrastructure.


## 3. System Architecture

- **Data Ingestion:** Employee data is ingested from multiple sources (CSV files) and stored in AWS S3.
- **Data Processing:** ETL pipelines process employee data daily, remove duplicates, and update records.
- **Real-time Communication Monitoring:** Kafka is used to stream employee communication data, with flagged messages being logged and tracked.
- **Storage:** Processed data is stored in relational databases using AWS RDS or PostgreSQL.
- **Reporting:** Automated scripts generate daily and monthly reports on employee activity and leave usage.


## 4. Technologies Used
- **Cloud Provider:** AWS (Amazon Web Services)
  - **Storage:** AWS S3
  - **Compute:** AWS EC2
  - **Database:** AWS RDS (PostgreSQL)
- **Data Processing:** Python, PySpark, Apache Airflow
- **Real-time Monitoring:** Apache Kafka
- **Reporting:** Python (pandas), SQL, PySpark
- **Other Tools:** Graphana, Aoache Airflow, AWS EMR


## 5. Setup Instructions

#### Prerequisites:
- AWS account with permissions for S3, EC2, RDS, and EMR.
- Python 3.x installed locally.
- Kafka (for local Kafka setup).

#### Steps to Setup:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/your-repo/employee-data-management-system.git
   cd employee-data-management-system
   ```

2. **Set up Python Environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure AWS Services:**
   - Create an S3 bucket for storing CSV files.
   - Set up RDS with PostgreSQL for database management.
   - Set up a Kafka instance (locally).

4. **Run Data Ingestion Pipelines:**
   - Modify the configurations in `config.py` to match your AWS environment.
   - Run the ETL scripts to process data:
     Eg-
     ```bash
     python daily_dag.py
     ```

## 6. How to Use

1. **Ingest Employee Data:** Upload daily employee CSV files (e.g., `employee_data.csv`) to the S3 bucket. The ETL process will pick up these files at scheduled intervals.
2. **View Reports:** Reports are generated daily and stored in the designated output folder or database table. The reports include data such as:
   - Active employees by designation.
   - Employees with potential leave abuse (more than 8% of working days).
3. **Monitor Employee Communication:** Kafka monitors communication streams and flags any messages that contain restricted words. Strikes are recorded, and employees with more than 10 strikes are marked as "INACTIVE."


## 7. Data Flow and Processing

- **Employee Data Ingestion:** Daily ingestion of employee data from CSV files into S3. The data is then processed to remove duplicates, manage employee status, and update the database.
- **Leave Data Processing:** Leave quota and calendar data are processed yearly, while leave requests are processed daily.
- **Employee Reporting:** A report of active employees and those with potential leave misuse is generated daily.


## 8. Reporting

- **Daily Report:** Shows active employees, their designations, and identifies employees with potential leave abuse.
- **Monthly Report:** On the 1st of every month, a report is generated that lists employees who have used more than 80% of their leave quota.
- **Communication Reports:** Logs of flagged messages are maintained, with salary deductions for each flagged message.


## 9. Communication Monitoring

- **Real-time Monitoring:** Kafka streams employee communication data in real time.
- **Flagging System:** Messages are checked against a list of reserved words. Flagged messages result in salary deductions and strikes.
- **Monthly Cooldown:** Employees have their strikes reset monthly unless they accumulate 10 strikes, in which case they are marked "INACTIVE."


## 10. Future Enhancements

- Implement actual email notifications for managers regarding employee leave status.
- Integrate machine learning to predict employee leave abuse patterns.
- Extend the system to monitor additional employee activities and behaviors.


## 11. Contributing

If you would like to contribute to this project, feel free to fork the repository, create a new branch, and submit a pull request. I am open to suggestions as I know I am a beginner and want to learn. So if you would like to help me improve, it would be appreciated.


Thank you for your time - Ananya Mishra
