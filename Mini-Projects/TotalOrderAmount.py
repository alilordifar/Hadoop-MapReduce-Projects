from mrjob.job import MRJob
from mrjobstep import MRStep


class TotalSpentByCustomer(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orderAmount,
                   reducer=self.reducer_order_total),
            MRStep(mapper=self.mapper_make_amount_key,
                   reducer=self.reducer_output_results)
        ]

    def mapper_get_orderAmount(self, _, line):
        (customerID, itemID, orderAmount) = line.split(',')
        yield customerID, float(orderAmount)

    def reducer_order_total(self, customerID, orders):
        yield customerID, sum(orders)

    def mapper_make_amount_key(self, customerID, orderTotal):
        yield '%04.02f' % float(orderTotal), customerID

    def reducer_output_results(self, orderTotal, customerIDs):
        for customerID in customerIDs:
            yield customerID, orderTotal


if __name__ == '__main__':
    TotalSpentByCustomer.run()
