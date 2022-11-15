class Employee:
    def __init__(self,
                 age,
                 gender,
                 salary,
                 monthly_expenditures,
                 occupation,
                 healthy_lifestyle):
        self.age = age
        self.gender = gender
        self.salary = salary
        self.monthly_expenditures = monthly_expenditures
        self.occupation = occupation
        self.healthy_lifestyle = healthy_lifestyle

    def __str__(self):
        return f"{self.age}/{self.gender}/{self.salary}"
