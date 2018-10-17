import workers.person as per


# -------------------------------------------group
class Group:
    def __init__(self, name, description, parent_group):
        if name is None:
            raise (per.DataError("Error group name"))
        else:
            self.name = name
        if description is None:
            raise (per.DataError("Error group description"))
        else:
            self.description = description
            self.parent_group = parent_group
            self.list = []

    def get_workers(self):
        if all(isinstance(x, Group) for x in self.list):
            workers = []
            for group in self.list:
                for i in group.get_workers():
                    workers.append(i)
        else:
            workers = [worker for worker in self.list]
        return workers


    def _get_par(self):
        if self.parent_group != None:
            yield self.parent_group

    def get_parents(self):
        result, parents = [], [self]
        while parents:
            node = parents.pop()
            if not node.parent_group is None:
                result.append(node.parent_group)
            parents.extend(node._get_par())
        return result


# -------------------------------------------worker
import workers.person as per
class Worker:
    def __init__(self, salary, person):
        if not isinstance(person, per.Person):
            print("person object")
            #raise (per.ErrorInData("error person object"))
        else:
            self.person = person

        self.__salary = salary

    def get_salary(self):
        return float(self.__salary)
# -------------------------------------------engineer
class Engineer(Worker):
    def __init__(self, salary, person, bonus):
        super(Engineer, self).__init__(salary, person)
        self.bonus = bonus

    def get_salary(self):
        return float(super(Engineer, self).get_salary()) + float(self.bonus)


# -------------------------------------------salesPerson
class SalesPerson(Worker):
    def __init__(self, salary, person, commission, deals):
        super(SalesPerson, self).__init__(salary, person)
        self.commission = commission
        self.deals = deals

    def get_salary(self):
        return float(super(SalesPerson, self).get_salary()) + float(self.commission) * sum(self.deals)


