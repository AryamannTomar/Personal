"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
exports.__esModule = true;
exports.ActiveUserCount = void 0;
var typescript_json_serializer_1 = require("typescript-json-serializer");
var ActiveUserCount = /** @class */ (function () {
    function ActiveUserCount(dau, wau, mau, daywisedau) {
        this.dau = dau;
        this.wau = wau;
        this.mau = mau;
        this.daywisedau = daywisedau;
    }
    __decorate([
        (0, typescript_json_serializer_1.JsonProperty)()
    ], ActiveUserCount.prototype, "dau");
    __decorate([
        (0, typescript_json_serializer_1.JsonProperty)()
    ], ActiveUserCount.prototype, "wau");
    __decorate([
        (0, typescript_json_serializer_1.JsonProperty)()
    ], ActiveUserCount.prototype, "mau");
    __decorate([
        (0, typescript_json_serializer_1.JsonProperty)()
    ], ActiveUserCount.prototype, "daywisedau");
    ActiveUserCount = __decorate([
        (0, typescript_json_serializer_1.JsonObject)()
    ], ActiveUserCount);
    return ActiveUserCount;
}());
exports.ActiveUserCount = ActiveUserCount;
