from mrjob.job import MRJob


class MaxTemp(MRJob):

    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit

    def mapper(self, _, line):
        StationID, date, field, temp, x, y, z, w)=line.split(',')
        if filed == 'TMAX':
            temperature=self.MakeFahrenheit(temp)
            yield stationId, temperature

    def reducer(self, stationID, temps):
        yield stationID, max(temps)

if __name__ == '__main__':
    MinTemp.run()
