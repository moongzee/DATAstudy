import cv2


img=cv2.imread(r'C:\Users\imagoworks-moongzeee\test\cul_img_mn_1.png')
img_gray=cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
ret, img_binary = cv2.threshold(img_gray, 127,255,0)
contours, hierachy = cv2.findContours(img_binary, cv2.RETR_TREE, cv2.CHAIN_APPROX_TC89_L1)

for cnt in contours:
    cv2.drawContours(img, [cnt], 0, (255,0,0), 2)

cv2.imshow('result',img)
cv2.waitKey(0)