source_schema_users = """
    id BIGINT,
    firstName STRING,
    lastName STRING,
    email STRING,
    address STRUCT<
        country: STRING
    >
"""

source_schema_products = """
    id BIGINT,
    title STRING,
    category STRING,
    brand STRING,
    price DECIMAL(18,2),
    discountPercentage DECIMAL(10,2),
    stock BIGINT
"""

source_schema_carts = """
    id BIGINT,
    userId BIGINT,
    products ARRAY<STRUCT<
        id: BIGINT,
        title: STRING,
        price: DECIMAL(18,2),
        quantity: BIGINT,
        total: DECIMAL(18,2),
        discountPercentage: DECIMAL(10,2),
        discountedTotal: DECIMAL(18,2),
        thumbnail: STRING
    >>,
    total DECIMAL(18,2),
    discountedTotal DECIMAL(18,2),
    totalProducts BIGINT,
    totalQuantity BIGINT
"""