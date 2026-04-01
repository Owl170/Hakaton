from backend.app.services.seed_service import ensure_seed_data


if __name__ == "__main__":
    ensure_seed_data()
    print("Seed data prepared")
