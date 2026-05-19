from motor.motor_asyncio import AsyncIOMotorDatabase


async def get_alerts(
    db: AsyncIOMotorDatabase,
    limit: int,
    severity: str | None,
) -> list[dict]:
    query: dict = {}
    if severity:
        query["severity"] = severity
    cursor = db["alerts"].find(query, {"_id": 0}).sort("triggered_at", -1).limit(limit)
    return await cursor.to_list(length=limit)
