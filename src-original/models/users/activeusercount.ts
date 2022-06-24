import { JsonObject, JsonProperty } from 'typescript-json-serializer';
/* 
  Create the ActiveUserCount Class Object for getting the desired output result of the function getUserCount 
*/
@JsonObject()
export class ActiveUserCount {
  @JsonProperty()
  dau: number;

  @JsonProperty()
  wau: number;

  @JsonProperty()
  mau: number;

  @JsonProperty()
  daywisedau: Array<object>;

  constructor(dau: number, wau: number, mau: number, daywisedau: Array<object>) {
    this.dau = dau;
    this.wau = wau;
    this.mau = mau;
    this.daywisedau = daywisedau;
  }
}