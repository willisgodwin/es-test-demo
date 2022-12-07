# from elasticsearch_dsl import Document, Keyword, Integer, Float, Text
#
#
# class Employee(Document):
#     age = Integer()
#     gender = Integer()
#     salary = Float()
#     monthly_expenditures = Float()
#     occupation = Text(analyzer="rebuild_standard")
#     healthy_lifestyle = Keyword()
#
#     class Index:
#         name = 'employees'
#         settings = {"number_of_shards": 2, "number_of_replicas": 1}
#
#     def __init__(self, age, gender, salary, monthly_expenditures, occupation, healthy_lifestyle, **kwargs):
#         super().__init__(**kwargs)
#         self.age = age
#         self.gender = gender
#         self.salary = salary
#         self.monthly_expenditures = monthly_expenditures
#         self.occupation = occupation
#         self.healthy_lifestyle = healthy_lifestyle
#
#     def __str__(self):
#         return f"{self.age}/{self.gender}/{self.salary}/{self.monthly_expenditures}/{self.occupation}/" \
#                f"{self.healthy_lifestyle}"


class EmployeeSearchFilter:
    def __init__(self,
                 age_filter: int = None,
                 gender_filter: int = None,
                 salary_range_filter_gte: float = None,
                 salary_range_filter_lte: float = None,
                 monthly_expenditures_range_filter_gte: float = None,
                 monthly_expenditures_range_filter_lte: float = None,
                 occupation_filter: str = None,
                 healthy_lifestyle_filter: str = None):
        self.age_filter = age_filter
        self.gender_filter = gender_filter
        self.salary_range_filter_gte = salary_range_filter_gte
        self.salary_range_filter_lte = salary_range_filter_lte
        self.monthly_expenditures_range_filter_gte = monthly_expenditures_range_filter_gte
        self.monthly_expenditures_range_filter_lte = monthly_expenditures_range_filter_lte
        self.occupation_filter = occupation_filter
        self.healthy_lifestyle_filter = healthy_lifestyle_filter

    def __str__(self):
        return f"{self.age_filter}/{self.gender_filter}/{self.salary_range_filter_lte}/" \
               f"{self.monthly_expenditures_range_filter_lte}/{self.occupation_filter} "
