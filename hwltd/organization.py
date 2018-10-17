import workers.structure as ws


class Employees:
    emp_list = {}

    def add(self, emp, email):
        Employees.emp_list[email] = emp


import workers.Person as per


class HelloWorld:
    helloWorld = ws.Group("Hello World ltd", "the organization", None)
    engineering = ws.Group("Engineering Department", "Engineering Department ", helloWorld)
    sw = ws.Group("Software Group", "develops software", engineering)
    infrastructure = ws.Group("Infrastructure Team", "does the Infrastructure", sw)
    app = ws.Group("App Team", "in charge of app development", sw)
    drivers = ws.Group("Drivers Team", "in charge of drivers", sw)
    qa = ws.Group("QA Team", "in charge of testing4", sw)
    sw.list = [infrastructure, app, drivers, qa]
    # ---------------------
    hw = ws.Group("Hardware Group", "develops Hardware", engineering)
    chip = ws.Group("Chip Team", "does the Chip", hw)
    board = ws.Group("Board Team", "does the Board", hw)
    power = ws.Group("Power Team", "does the Power", hw)
    hw.list[chip, board, power]
    # --------------------
    cto = ws.Group("CTO Group", "cto", engineering)
    # --------------------
    system = ws.Group("System Group", "work in the System", engineering)
    design = ws.Group("Design Team", "does the Design", system)
    poc = ws.Group("Poc Team", "does the Poc", system)
    system.list[design, poc]

    engineering.list = [sw, hw, cto, system]
    # --------------------
    hr = ws.Group("HR Department", "Hr department", helloWorld)
    rec = ws.Group("Recruitment Group", "Recruitment Group", hr)
    tech = ws.Group("Tech Team", "does the tech recruitment", rec)
    staff = ws.Group("Staff Team", "does the staff recruitment", rec)
    rec.list = [tech, staff]

    culture = ws.Group("Culture Group", "Culture Group", hr)

    hr.list = [rec, culture]

    # -----------------------
    finance = ws.Group("Finance Department", "cares for financing", helloWorld)
    salaries = ws.Group("Salaries Group", "in charge of salaries", finance)

    budget = ws.Group("Budget Group", "in charge of Budget", finance)
    income = ws.Group("Income Team", "deals with income", budget)
    outcome = ws.Group("Outcome Team", "deals with outcome", budget)
    budget.list = [income, outcome]

    finance.list = [salaries, budget]

    helloWorld.list = [engineering, hr, finance]

    # ------------------------
    def __init__(self, path):
        self.path = path
        self.employees = Employees()
        with open(path) as f:
            for line in f.readlines():
                try:
                    res = self.add(line)
                    if not res is None:
                        self.employees.add(res, res.person.get_email())
                except per.DataError:
                    continue

    # ------------------------
    def add(self, line):
        words = line.split(', ')
        if not words[0].startswith('#') and words[0] != '\n':
            phones = words[4].split(";")
            address = words[5].split(";")
        if len(address) == 4:
            faddress = per.StreetAddress(address[0], address[1], address[2], address[3])
        elif len(address) == 5:
            faddress = per.StreetAddress(address[0], address[1], address[2])
        else:
            raise (per.DataError("the address is not valid"))
        person = per.Person(words[0], words[1], words[2], words[3], phones, faddress)
        money = words[8].split(";")
        emp = None
        if words[7] == "staff":
            if len(money) != 1:
                raise (per.DataError("staff address vars"))
            else:
                emp = ws.Worker(person, words[8])
        elif words[7] == "engineer":
            if len(money) != 2:
                raise (per.DataError("engineer address vars"))
            else:
                emp = ws.Engineer(person, money[0], money[1])
        elif words[7] == "sales":
            if len(money) < 3:
                raise (per.DataError("salesperson address vars"))
            else:
                money = words[8].split(";")
                deals = []
                for i in range(2, len(money)):
                    deals.append(money[i])

                emp = ws.SalesPerson(person, money[0], money[1], deals)
        if (words[6] == "qa"):
            self.qa.list.append(emp)
        elif (words[6] == "infrastructure"):
            self.infrastructure.list.append(emp)
        elif (words[6] == "app"):
            self.app.list.append(emp)
        elif (words[6] == "drivers"):
            self.drivers.list.append(emp)
        elif (words[6] == "chip"):
            self.chip.list.append(emp)
        elif (words[6] == "board"):
            self.board.list.append(emp)
        elif (words[6] == "power"):
            self.power.list.append(emp)
        elif (words[6] == "cto"):
            self.cto.list.append(emp)
        elif (words[6] == "design"):
            self.design.list.append(emp)
        elif (words[6] == "poc"):
            self.poc.list.append(emp)
        elif (words[6] == "tech"):
            self.tech.list.append(emp)
        elif (words[6] == "staff"):
            self.staff.list.append(emp)
        elif (words[6] == "salaries"):
            self.salaries.list.append(emp)
        elif (words[6] == "income"):
            self.income.list.append(emp)
        elif (words[6] == "outcome"):
            self.outcome.list.append(emp)
        elif (words[6] == "culture"):
            self.culture.list.append(emp)
        else:
            raise per.DataError("team")




