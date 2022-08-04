var DataSketches = require('./a');
var ds = new DataSketches();
test('Result', async () => {
    expect(
        await ds.userCount('2021-09-01', '2021-09-29', '5bebe93c25d705690ffbc75811'),
    ).toEqual(
        expect.objectContaining({
            DAU: expect.any(Number),
            WAU: expect.any(Number),
            MAU: expect.any(Number),
            daywisedau: expect.any(Array)
        }))
})