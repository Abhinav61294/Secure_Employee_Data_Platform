import random
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException


app = FastAPI(
    title="Employee Compliance API",
    description="Mock employee compliance REST API",
    version="1.0.0",
)


@app.get("/")
def root():
    return {
        "message": "Employee Compliance API is running"
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy"
    }


@app.get("/employees/{employee_id}")
def get_employee_compliance(employee_id: str):
    """Return synthetic compliance information for an employee."""

    if not employee_id.startswith("EMP"):
        raise HTTPException(
            status_code=400,
            detail="Invalid employee ID format",
        )

    training_status = random.choice(
        [
            "Completed",
            "Completed",
            "Completed",
            "Pending",
            "Expired",
        ]
    )

    background_check_status = random.choice(
        [
            "Cleared",
            "Cleared",
            "Cleared",
            "Pending",
        ]
    )

    compliance_status = (
        "Compliant"
        if training_status == "Completed"
        and background_check_status == "Cleared"
        else "Non-Compliant"
    )

    start_date = date(2024, 1, 1)
    end_date = date(2026, 9, 1)

    days_between = (end_date - start_date).days

    last_review_date = start_date + timedelta(
        days=random.randint(0, days_between)
    )

    return {
        "employee_id": employee_id,
        "compliance_status": compliance_status,
        "background_check_status": background_check_status,
        "training_status": training_status,
        "last_review_date": last_review_date.isoformat(),
    }
@app.get("/")
def root():
    return {
        "message": "Employee Compliance API is running"
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy"
    }