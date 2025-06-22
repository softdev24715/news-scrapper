# Add all spiders to spider_status if they don't exist
all_spiders = [
    # News spiders (13)
    'tass', 'rbc', 'vedomosti', 'pnp', 'lenta', 'graininfo', 'forbes', 
    'interfax', 'izvestia', 'gazeta', 'rg', 'kommersant', 'ria', 'meduza',
    # Government and official spiders (4)
    'government', 'kremlin', 'regulation',
    # Legal document spiders (3)
    'pravo', 'sozd', 'eaeu'
]

for spider in all_spiders:
    cursor.execute("""
        INSERT INTO spider_status (name, status) 
        VALUES (%s, 'enabled') 
        ON CONFLICT (name) DO NOTHING
    """, (spider,))

print("✅ Added all 20 spiders to spider_status") 