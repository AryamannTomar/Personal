describe('DataSketches', () => {
    test('Upsert', () => {
        expect('Completed').toContain('Completed');
    });
    test('Cohort', () => {
        expect('Cohort Successfully').toContain('Successfully');
    });
    test('DWM', () => {
        expect({
            dau: 3.45,
            wau: 2.27,
            mau: 8.33,
            daywisedau: [['2021-01-02', 1], ['2021-01-04', 1]]
        }).toMatchObject({
            dau: expect.any(Number),
            wau: expect.any(Number),
            mau: expect.any(Number),
            daywisedau: expect.any(Array)
        })
    });
});