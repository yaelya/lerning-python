#import workers.person as per
import workers.structure as struct
#import hwltd.organization as org
#import hwltd.reports as rep
import workers.person as per


def main():
    print(__name__)
    x = per.Person("yael","yyy",1996,"yael.yazdi96@jhr.hwltd.com",["77777"],"katza21")
    print("%s %s %d %s %s %s" % (x.getFirstName(),x.getLastName(),x.year_of_birth,x.get_id(),x.phones,x.address))
    x2 = per.Person("Kramersky", "Vosmo", 1984, "dtrump@serv.hwltd.com", ["+22264-8866024"], "Kermany;Berlit;9188")
    print("%s %s %d %d %s %s" % (x2.getFirstName(), x2.getLastName(), x2.year_of_birth, x2.get_id(), x2.phones,x2.address))
    x.add_number("08889999")
    #x.remove_number("6514253")//not work
    print("%s %s %d %d %s %s" % (x.getFirstName(), x.getLastName(), x.year_of_birth, x.get_id(), x.phones, x.address))
    y = per.Phone("0009999")
    print(y.number)

    k = per.StreetAddress("jerusalem", "israel", "mm", 9)
    print(k.getAddress())

    l = per.PobAddrees("israel", "bersheva", "9888")
    print(l.getAddress())

    deals = ["hh", "gg", "pp"]
    w1 = struct.SalesPerson(x, 100000, 1000, deals)
    print(w1.get_salary())




if __name__ == '__main__':
    main()


    # deals=["hh","gg","pp"]
    # w1=strc.SalesPerson(x,100000,1000,deals)
    # print(w1.get_salary())

   # c=org.HelloWorld(r"C:\Users\נעמי פינדרוס\Downloads\Data 2.txt")
    #print(c.employees.emp_list)
    #print([i.person.get_first_name() for i  in c.budget.get_workers()])
    #print([i.name for i in c.poc.get_parents()])
    #print(rep.get_average_salary(c.sw))
    #rep.get_num_employees(org.HelloWorld.helloWorld,4)
    #print( rep.get_relational_salary(org.Employees.emp_list["gb@tech.hwltd.com"]))


