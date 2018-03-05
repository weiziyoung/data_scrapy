from scrapy import Item, Field


class JobItem(Item):
    url = Field()
    name = Field()
    location = Field()
    salary = Field()
    pub_date = Field()
    available_number = Field()
    job_category = Field()
    exp = Field()
    edu = Field()
    duty = Field()
    demand = Field()
    welfare = Field()
    work_time = Field()
    address = Field()
    company_name = Field()
    company_scale = Field()
    company_type = Field()
    company_industry = Field()
    from_website = Field()
    scrape_time = Field()
