from motor.motor_asyncio import AsyncIOMotorDatabase


async def get_stats(
    db: AsyncIOMotorDatabase,
    pair: str | None,
    window: str,
) -> list[dict]:
    query: dict = {"window": window}
    if pair:
        query["pair"] = pair
    cursor = db["aggregates"].find(query, {"_id": 0})
    return await cursor.to_list(length=100)
