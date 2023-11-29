class Company:
    def __init__(self, id, name,domain, year_founded, industry, size_range, locality, country, 
                 linkedin_url, current_employee_estimate,total_employee_estimate):
        self.id = id
        self.name = name
        self.domain = domain
        self.year_founded = year_founded
        self.industry = industry
        self.size_range = size_range
        self.locality = locality
        self.country = country
        self.linkedin_url = linkedin_url
        self.current_employee_estimate = current_employee_estimate
        self.total_employee_estimate = total_employee_estimate