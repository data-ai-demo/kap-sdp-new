"""Shared constants for fixture data generation.

Provides consistent agency codes, carrier names, and business names
across all generators so cross-source joins are possible at Silver layer.
"""

import random

RANDOM_SEED = 42

# ─── 40 AGENCY CODES ───
# Format: KAP-XXXX (4-digit numeric)
AGENCY_CODES = [f"KAP-{i:04d}" for i in range(1001, 1041)]

# ─── 12+ CARRIER NAMES ───
CARRIER_NAMES = [
    "Acme Insurance Co",
    "Liberty National Group",
    "Summit Underwriters",
    "Beacon Mutual",
    "Patriot Indemnity",
    "Cornerstone Surety",
    "Ironclad Casualty",
    "Meridian P&C",
    "Pinnacle Risk",
    "Vanguard General",
    "Atlantic Specialty",
    "Evergreen Assurance",
    "Frontier Mutual",
    "Harbor Point Re",
    "Keystone Preferred",
]

# ─── 80+ BUSINESS NAMES (insured entities) ───
BUSINESS_NAMES = [
    "Apex Manufacturing LLC",
    "Blue Ridge Construction",
    "Cascade Logistics Inc",
    "Delta Welding Services",
    "Evergreen Landscaping Co",
    "Falcon Transport Group",
    "Granite State Plumbing",
    "Harbor View Restaurant",
    "Ironworks Steel Fabrication",
    "Jetstream Aviation Services",
    "Keystone Auto Body",
    "Lakeside Marina LLC",
    "Maple Grove Dairy Farm",
    "Northstar Electric",
    "Oakwood Furniture Co",
    "Pacific Rim Seafood",
    "Quantum Tech Solutions",
    "Redwood Timber Inc",
    "Silverline Trucking",
    "Titan Roofing Corp",
    "Uptown Dental Group",
    "Valley Fresh Produce",
    "Westfield Properties",
    "Xavier Engineering",
    "Yellowstone Outfitters",
    "Zenith Solar Energy",
    "Alpine Ski Resort LLC",
    "Broadview Medical Center",
    "Crossroads Retail Group",
    "Dogwood Nursery",
    "Eclipse Software Inc",
    "Firehouse Pizza Chain",
    "Golden Harvest Farms",
    "Highland Brewing Co",
    "Interstate Paving",
    "Juniper Creek Ranch",
    "Kensington Hotels",
    "Liberty Bell Bakery",
    "Mountainview Excavation",
    "Neptune Fisheries",
    "Orchard Hill Winery",
    "Prairie Wind Energy",
    "Quarry Stone Supply",
    "Riverbend Golf Club",
    "Stonewall Masonry",
    "Trident Marine Services",
    "Unity Health Clinic",
    "Vista Ridge Apartments",
    "Wildflower Catering",
    "Xpress Courier LLC",
    "York County Hardware",
    "Zephyr Air Charter",
    "Anchor Point Shipping",
    "Birchwood Cabinet Co",
    "Cliffside Excavating",
    "Driftwood Furniture",
    "Eastlake Plumbing",
    "Foxglove Garden Center",
    "Glacier Moving Co",
    "Hawthorne Real Estate",
    "Inlet Bay Charters",
    "Jasper Stone Works",
    "Kodiak Construction",
    "Lonestar Drilling",
    "Mosaic Tile Studio",
    "Northwind HVAC",
    "Outpost Camping Supply",
    "Pinecrest Logging",
    "Quail Run Equestrian",
    "Ridgeline Roofing",
    "Sandstone Quarry Inc",
    "Tidewater Electric",
    "Underhill Farms",
    "Vineland Produce",
    "Whitewater Rafting Co",
    "Xylem Irrigation",
    "Yosemite Trail Guides",
    "Zion Landscaping",
    "Ashford Textiles",
    "Bayshore Crab House",
    "Cedar Falls Lumber",
    "Dusty Road Ranch",
]

# ─── COVERAGE TYPES ───
COVERAGE_TYPES = [
    "General Liability",
    "Commercial Property",
    "Workers Compensation",
    "Commercial Auto",
    "Professional Liability",
    "Umbrella/Excess",
    "Inland Marine",
    "Business Owners Policy",
]

# ─── US STATES (for addresses) ───
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
]


def seeded_random():
    """Return a seeded Random instance for reproducible fixture generation."""
    return random.Random(RANDOM_SEED)
