import hwltd.organization as org
import workers.structure as ws


def get_num_employees(department, depth):
    if depth <= 0:
        raise ws.per.ErrorInData("depth is lest than one!")
    else:
        print("%s :%s" % (department.name, len(department.get_workers())))
        if depth > 1:
            for i in department.list:
                print(" %s :%s" % (i.name, len(i.get_workers())))
                if i.list is not None and depth >= 3:
                    for j in i.list:
                        if isinstance(j, ws.Group):
                            print("  %s :%s" % (j.name, len(j.get_workers())))
                        if j.list is not None and depth >= 4:
                            for k in j.list:
                                if isinstance(k, ws.Group):
                                    print("   %s :%s" % (k.name, len(k.get_workers())))


# --------------------------------
def get_average_salary(group):
    workers = group.get_workers()
    sumSalary = 0
    for i in workers:
        sumSalary = float(sumSalary) + float(i.get_salary())
    return sumSalary / sum(myWorkers)


# --------------------------------
def get_relational_salary(worker):
    group_list = [org.HelloWorld.infrastructure, org.HelloWorld.app, org.HelloWorld.qa, org.HelloWorld.drivers,
                  org.HelloWorld.chip
        , org.HelloWorld.power, org.HelloWorld.board, org.HelloWorld.cto, org.HelloWorld.design, org.HelloWorld.poc,
                  org.HelloWorld.tech, org.HelloWorld.staff, org.HelloWorld.culture, org.HelloWorld.salaries,
                  org.HelloWorld.income, org.HelloWorld.outcome]
    w_dict = {}
    worker_sal = worker.get_salary()
    for j in group_list:
        if worker in j.get_workers():
            for wr in j.get_workers():
                if wr != worker:
                    w_sal = wr.get_salary()
                    w_dict[wr.person.get_first_name()] = float(w_sal / worker_sal)

    return w_dict


