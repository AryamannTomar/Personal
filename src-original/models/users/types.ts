import { JsonObject, JsonProperty } from 'typescript-json-serializer';
/* 
  Create the DAU, WAU Class Object for defining models for the functions getDAU, getWAU and getMAU 
*/

@JsonObject()
export class DAU{
    @JsonProperty()
    DAU: number;

    @JsonProperty()
    daywisedau: Array<any>;

    constructor(DAU: number, daywisedau: Array<any>) {
        this.DAU = DAU;
        this.daywisedau = daywisedau;
      }
}

@JsonObject()
export class WAU{
  @JsonProperty()
  WAU: number;

  constructor(WAU: number) {
    this.WAU = WAU;
  }
};

@JsonObject()
export class MAU{
  @JsonProperty()
  MAU: number;

  constructor(MAU: number) {
    this.MAU = MAU;
  }
};
