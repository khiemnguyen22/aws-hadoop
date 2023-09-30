from mrjob.job import MRJob

class MRSalaries(MRJob):

    def mapper(self, _, line):
        (name,jobTitle,agencyID,agency,hireDate,annualSalary,grossPay) = line.split('\t')
        salary = float(annualSalary)
        if salary >= 100000.00:
            yield 'High', 1
        elif 50000.00 <= salary <= 99999.99:
            yield 'Medium', 1
        elif 0.00 <= salary <= 49999.99:
            yield 'Low', 1

    def combiner(self, jobTitle, counts):
        yield jobTitle, sum(counts)

    def reducer(self, jobTitle, counts):
        yield jobTitle, sum(counts)

if __name__ == '__main__':
    MRSalaries.run()
